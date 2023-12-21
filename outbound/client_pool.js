'use strict';

const utils        = require('haraka-utils');
const { Pool }     = require('tarn');

const sock         = require('../line_socket');
const logger       = require('../logger');

const obc          = require('./config');
const server       = require('../server');


function _create_socket (pool_name, port, host, local_addr, is_unix_socket, callback) {
    const name = `outbound::${port}:${host}:${local_addr}`;
    const socket = is_unix_socket ? sock.connect({path: host}) : sock.connect({port, host, localAddress: local_addr});
    socket.name = name;
    socket.__pool_name = pool_name;
    socket.__uuid = utils.uuid();
    socket.__in_use = true; // Used by the pool to determine if this socket is in use or needs to be destroyed
    socket.setTimeout(obc.connect_timeout * 1000);
    logger.logdebug(`[outbound] created. host: ${host} port: ${port}`, { uuid: socket.__uuid });
    socket.once('connect', () => {
        socket.removeAllListeners('error'); // these get added after callback
        socket.removeAllListeners('timeout');
        callback(null, socket);
    });
    socket.once('error', err => {
        socket.end();
        socket.removeAllListeners();
        socket.destroy();
        callback(`Outbound connection error: ${err}`, null);
    });
    socket.once('timeout', () => {
        socket.end();
        socket.removeAllListeners();
        socket.destroy();
        callback(`Outbound connection timed out to ${host}:${port}`, null);
    });
}

function get_pool (port, host, local_addr, is_unix_socket, maxConnections) {
    port = port || 25;
    host = host || 'localhost';
    const name = `pool:outbound::${port}:${host}:${local_addr}`;

    if (!server.notes.pool) {
        server.notes.pool = {};
    }

    let pool_timeout = obc.pool_timeout;

    // Search host in pool list and use custom pool settings if found
    const poolCfg = obc.pools.find(p => p.host.includes(host));
    if (poolCfg) {
        maxConnections = poolCfg.maxConnections;
        pool_timeout = poolCfg.pool_timeout;
    }

    // Create pool if not exists
    if (!server.notes.pool[name]) {
        const pool = new Pool({
            // Function that creates a resource. You can either pass the resource
            // to the callback(error, resource) or return a promise that resolves the resource
            // (but not both) Callback syntax will be deprecated at some point.
            create: cb => {
                _create_socket(name, port, host, local_addr, is_unix_socket, cb);
            },
            // Validates a connection before it is used. Return true or false
            // from it. If false is returned, the resource is destroyed and
            // another one is acquired. Should return a Promise if validate is
            // an async function.
            validate: socket => {
                return socket.__in_use && socket.writable;
            },
            // Function that destroys a resource, should return a promise if
            // destroying is an asynchronous operation.
            destroy: socket => {
                logger.logdebug(`[outbound] destroying pool entry ${socket.__uuid} for ${host}:${port}`);
                socket.removeAllListeners();
                socket.__in_use = false;
                socket.on('line', function (line) {
                    // Just assume this is a valid response
                    logger.logprotocol(`[outbound] S: ${line}`);
                });
                socket.once('error', function (err) {
                    logger.logwarn(`[outbound] Socket got an error while shutting down: ${err}`);
                });
                socket.once('end', function () {
                    logger.loginfo("[outbound] Remote end half closed during destroy()");
                    socket.destroy();
                })
                if (socket.writable) {
                    logger.logprotocol(`[outbound] [${socket.__uuid}] C: QUIT`);
                    socket.write("QUIT\r\n");
                }
                socket.end(); // half close
            },
            // logger function, noop by default
            log: (message, logLevel) => logger.logdebug(`${logLevel}: ${message}`),
            // minimum size
            min: 1,
            // maximum size
            max: maxConnections,
            // acquire promises are rejected after this many milliseconds
            // if a resource cannot be acquired
            acquireTimeoutMillis: 30000,
            // create operations are cancelled after this many milliseconds
            // if a resource cannot be acquired
            createTimeoutMillis: 30000,
            // destroy operations are awaited for at most this many milliseconds
            // new resources will be created after this timeout
            destroyTimeoutMillis: 5000,
            // Free resources are destroyed after this many milliseconds.
            // Note that if min > 0, some resources may be kept alive for longer.
            // To reliably destroy all idle resources, set min to 0.
            idleTimeoutMillis: pool_timeout * 1000,
            // how often to check for idle resources to destroy
            reapIntervalMillis: 1000,
            // how long to idle after failed create before trying again
            createRetryIntervalMillis: 200,
            // If true, when a create fails, the first pending acquire is
            // rejected with the error. If this is false (the default) then
            // create is retried until acquireTimeoutMillis milliseconds has
            // passed.
            propagateCreateError: false
        });
        server.notes.pool[name] = pool;
    }

    return server.notes.pool[name];
}


// Get a socket for the given attributes.
exports.get_client = (port = 25, host = 'localhost', local_addr, is_unix_socket, callback) => {

    if (obc.pool_concurrency_max == 0) {
        return _create_socket(null, port, host, local_addr, is_unix_socket, callback);
    }

    const pool = get_pool(port, host, local_addr, is_unix_socket, obc.pool_concurrency_max);
    if (pool.numFree() >= obc.pool_concurrency_max) {
        return callback("Too many waiting clients for pool", null);
    }

    pool.acquire().then(socket => {
        socket.__acquired = true;
        socket.removeAllListeners('error'); // these get added after callback
        socket.removeAllListeners('timeout');
        callback(null, socket);
    }).catch(err => {
        callback(err, null);
    });
}

function releaseSocket (socket) {
    const name = socket.__pool_name;
    socket.__in_use = false;
    if (server.notes.pool && server.notes.pool[name]) {
        server.notes.pool[name].destroy(socket);
    }
    else {
        socket.removeAllListeners();
        socket.destroy();
    }
}

exports.release_client = (socket, port, host, local_addr, error) => {
    logger.logdebug(`[outbound] release_client: ${socket.__uuid} ${host}:${port} to ${local_addr}`);
    const name = socket.__pool_name;

    if (!name && obc.pool_concurrency_max == 0) {
        return releaseSocket(socket);
    }

    if (!socket.__acquired) {
        logger.logwarn(`Release an un-acquired socket. Stack: ${(new Error()).stack}`);
        return;
    }
    socket.__acquired = false;

    if (!(server.notes && server.notes.pool)) {
        logger.logcrit(`[outbound] Releasing a pool (${name}) that doesn't exist!`);
        return;
    }

    const pool = server.notes.pool[name];
    if (!pool) {
        logger.logcrit(`[outbound] Releasing a pool (${name}) that doesn't exist!`);
        return;
    }

    if (error) {
        return releaseSocket(socket);
    }

    if (obc.pool_timeout == 0) {
        logger.loginfo("[outbound] Pool_timeout is zero - shutting it down");
        return releaseSocket(socket);
    }

    socket.removeAllListeners('close');
    socket.removeAllListeners('error');
    socket.removeAllListeners('end');
    socket.removeAllListeners('timeout');
    socket.removeAllListeners('line');

    socket.once('error', function (err) {
        logger.logwarn(`[outbound] Socket [${name}] in pool got an error: ${err}`);
        releaseSocket(socket);
    });

    socket.once('end', function () {
        logger.loginfo(`[outbound] Socket [${name}] in pool got FIN`);
        socket.writable = false;
        releaseSocket(socket);
    });

    pool.release(socket);
}

exports.drain_pools = function () {
    if (!server.notes.pool || Object.keys(server.notes.pool).length == 0) {
        return logger.logdebug("[outbound] Drain pools: No pools available");
    }

    const tasks = Object.keys(server.notes.pool).map(function (poolName) {
        logger.logdebug(`[outbound] Drain pools: Draining SMTP connection pool ${poolName}`);
        const pool = server.notes.pool[poolName];
        delete server.notes.pool[poolName];
        return pool.destroy();
    });

    return Promise.all(tasks).then(() => {
        logger.logdebug("[outbound] Drain pools: All pools shut down");
    }).catch(err => {
        logger.logerror(`[outbound] Drain pools: Error shutting down pools: ${err}`);
    });
}
