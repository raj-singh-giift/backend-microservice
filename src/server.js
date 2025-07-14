import http from 'http';
import https from 'https';
import fs from 'fs-extra';
import path from 'path';
import debug from 'debug';

import config from './config/index.js';
import logger from './config/logger.js';

const debugServer = debug('app:server');

/**
 * Create HTTPS server with SSL certificates
 * @param {Express} app - Express application
 * @returns {https.Server} HTTPS server instance
 */
const createHttpsServer = async (app) => {
    try {
        const certPath = path.resolve(config.ssl.certPath);
        const keyPath = path.resolve(config.ssl.keyPath);

        // Check if SSL certificates exist
        const certExists = await fs.pathExists(certPath);
        const keyExists = await fs.pathExists(keyPath);

        if (!certExists || !keyExists) {
            throw new Error(`SSL certificates not found. Cert: ${certExists}, Key: ${keyExists}`);
        }

        const options = {
            cert: await fs.readFile(certPath, 'utf8'),
            key: await fs.readFile(keyPath, 'utf8'),
            // Additional SSL options for production
            secureProtocol: 'TLSv1_2_method',
            ciphers: [
                'ECDHE-RSA-AES128-GCM-SHA256',
                'ECDHE-RSA-AES256-GCM-SHA384',
                'ECDHE-RSA-AES128-SHA256',
                'ECDHE-RSA-AES256-SHA384'
            ].join(':'),
            honorCipherOrder: true
        };

        const server = https.createServer(options, app);
        debugServer('HTTPS server created');
        return server;
    } catch (error) {
        logger.error('Failed to create HTTPS server:', error);
        throw error;
    }
};

/**
 * Create HTTP server
 * @param {Express} app - Express application
 * @returns {http.Server} HTTP server instance
 */
const createHttpServer = (app) => {
    const server = http.createServer(app);
    debugServer('HTTP server created');
    return server;
};

/**
 * Setup server event listeners
 * @param {http.Server|https.Server} server - Server instance
 */
const setupServerEvents = (server) => {
    server.on('error', (error) => {
        if (error.syscall !== 'listen') {
            throw error;
        }

        const bind = typeof config.port === 'string'
            ? `Pipe ${config.port}`
            : `Port ${config.port}`;

        switch (error.code) {
            case 'EACCES':
                logger.error(`${bind} requires elevated privileges`);
                process.exit(1);
                break;
            case 'EADDRINUSE':
                logger.error(`${bind} is already in use`);
                process.exit(1);
                break;
            default:
                logger.error('Server error:', error);
                throw error;
        }
    });

    server.on('listening', () => {
        const addr = server.address();
        const bind = typeof addr === 'string'
            ? `pipe ${addr}`
            : `port ${addr.port}`;

        const protocol = config.useHttps ? 'HTTPS' : 'HTTP';
        logger.info(`${config.app.name} ${protocol} server listening on ${bind}`);
        debugServer(`Server listening on ${bind}`);
    });

    server.on('connection', (socket) => {
        debugServer('New connection established');

        // Set socket timeout
        socket.setTimeout(30000);

        socket.on('timeout', () => {
            debugServer('Socket timeout');
            socket.destroy();
        });
    });

    server.on('clientError', (error, socket) => {
        logger.warn('Client error:', error.message);
        if (socket.writable) {
            socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
        }
    });
};

/**
 * Setup graceful shutdown for server
 * @param {http.Server|https.Server} server - Server instance
 */
const setupGracefulShutdown = (server) => {
    let isShuttingDown = false;

    const gracefulShutdown = (signal) => {
        if (isShuttingDown) return;
        isShuttingDown = true;

        logger.info(`${signal} received, shutting down gracefully`);
        debugServer('Starting graceful shutdown');

        // Stop accepting new connections
        server.close((error) => {
            if (error) {
                logger.error('Error during server shutdown:', error);
                process.exit(1);
            }

            logger.info('Server closed successfully');
            debugServer('Server shutdown completed');
            process.exit(0);
        });

        // Force close after timeout
        setTimeout(() => {
            logger.error('Could not close connections in time, forcefully shutting down');
            process.exit(1);
        }, 30000);
    };

    process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
    process.on('SIGINT', () => gracefulShutdown('SIGINT'));
};

/**
 * Start the server
 * @param {Express} app - Express application
 * @returns {Promise<http.Server|https.Server>} Server instance
 */
export const startServer = async (app) => {
    try {
        let server;

        // Create server based on environment configuration
        if (config.useHttps) {
            server = await createHttpsServer(app);
            logger.info('Starting HTTPS server...');
        } else {
            server = createHttpServer(app);
            logger.info('Starting HTTP server...');
        }

        // Setup server event listeners
        setupServerEvents(server);
        setupGracefulShutdown(server);

        // Start listening
        server.listen(config.port, () => {
            const protocol = config.useHttps ? 'https' : 'http';
            logger.info(`🚀 Server ready at ${protocol}://localhost:${config.port}`);

            if (config.env === 'development') {
                console.log(`
╔════════════════════════════════════════╗
║            SERVER STARTED              ║
║                                        ║
║  Environment: ${config.env.padEnd(24)} ║
║  Protocol:    ${(config.useHttps ? 'HTTPS' : 'HTTP').padEnd(24)} ║
║  Port:        ${config.port.toString().padEnd(24)} ║
║  URL:         ${protocol}://localhost:${config.port.toString().padEnd(12)} ║
║                                        ║
║  Health:      ${protocol}://localhost:${config.port}/health  ║
║  API Docs:    ${protocol}://localhost:${config.port}/api     ║
╚════════════════════════════════════════╝
        `);
            }
        });

        return server;
    } catch (error) {
        logger.error('Failed to start server:', error);
        process.exit(1);
    }
};

/**
 * Create a development server with hot reload support
 * @param {Express} app - Express application
 * @returns {Promise<http.Server>} Server instance
 */
export const startDevServer = async (app) => {
    if (config.env !== 'development') {
        throw new Error('Development server can only be started in development environment');
    }

    logger.info('Starting development server with enhanced logging...');

    // Enhanced logging for development
    app.use((req, res, next) => {
        const start = Date.now();

        res.on('finish', () => {
            const duration = Date.now() - start;
            debugServer(`${req.method} ${req.originalUrl} - ${res.statusCode} (${duration}ms)`);
        });

        next();
    });

    return startServer(app);
};

export default { startServer, startDevServer };