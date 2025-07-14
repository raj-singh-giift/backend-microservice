import express from 'express';
import helmet from 'helmet';
import cors from 'cors';
import compression from 'compression';
import cookieParser from 'cookie-parser';
import morgan from 'morgan';
import trimRequest from 'trim-request';
import path from 'path';
import fs from 'fs-extra';
import debug from 'debug';

// Configuration and utilities
import config from './config/index.js';
import logger from './config/logger.js';
import { connectDatabase } from './config/database.js';
import { connectRedis } from './config/redis.js';

// Middleware
import { requestTracker } from './middleware/requestTracker.js';
import { securityMiddleware } from './middleware/security.js';
import { rateLimiter } from './middleware/rateLimiter.js';
import { errorHandler, notFoundHandler } from './middleware/errorHandler.js';
import { validationErrorHandler } from './middleware/validation.js';

// Routes
import routes from './routes/index.js';

// Services
import './services/cronService.js'; // Initialize cron jobs

const app = express();
const debugApp = debug('app:main');

// Trust proxy for accurate client IP
app.set('trust proxy', 1);

// View engine setup for EJS templates
app.set('views', path.join(process.cwd(), 'views'));
app.set('view engine', 'ejs');

// Create necessary directories
const ensureDirectories = async () => {
    try {
        await fs.ensureDir('logs');
        await fs.ensureDir('uploads');
        await fs.ensureDir('public');
        debugApp('Directories ensured');
    } catch (error) {
        logger.error('Failed to create directories:', error);
    }
};

// Core middleware setup
const setupMiddleware = () => {
    // Request tracking (must be first)
    app.use(requestTracker);

    // Security middleware
    app.use(helmet({
        contentSecurityPolicy: {
            directives: {
                defaultSrc: ["'self'"],
                styleSrc: ["'self'", "'unsafe-inline'"],
                scriptSrc: ["'self'"],
                imgSrc: ["'self'", "data:", "https:"],
            },
        },
        hsts: {
            maxAge: 31536000,
            includeSubDomains: true,
            preload: true
        }
    }));

    // CORS configuration
    const corsOptions = {
        origin: function (origin, callback) {
            const allowedOrigins = config.cors.origins;
            if (!origin || allowedOrigins.includes(origin)) {
                callback(null, true);
            } else {
                callback(new Error('Not allowed by CORS'));
            }
        },
        credentials: config.cors.credentials,
        methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
        allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With'],
        exposedHeaders: ['X-Total-Count', 'X-Page-Count']
    };
    app.use(cors(corsOptions));

    // Compression
    app.use(compression({
        filter: (req, res) => {
            if (req.headers['x-no-compression']) return false;
            return compression.filter(req, res);
        },
        level: 6,
        threshold: 1024
    }));

    // Rate limiting
    app.use(rateLimiter);

    // Morgan HTTP logging
    const morganFormat = config.env === 'production' ? 'combined' : 'dev';
    app.use(morgan(morganFormat, {
        stream: {
            write: (message) => logger.info(message.trim(), { service: 'http' })
        },
        skip: (req, res) => config.env === 'test'
    }));

    // Body parsing
    app.use(express.json({
        limit: '10mb',
        strict: true
    }));
    app.use(express.urlencoded({
        extended: true,
        limit: '10mb',
        parameterLimit: 1000
    }));

    // Cookie parsing
    app.use(cookieParser(config.security.cookieSecret));

    // Trim request data
    app.use(trimRequest.all);

    // Static files
    app.use('/public', express.static(path.join(process.cwd(), 'public'), {
        maxAge: config.env === 'production' ? '1d' : 0,
        etag: true
    }));

    // Additional security middleware
    app.use(securityMiddleware);

    debugApp('Middleware setup completed');
};

// Routes setup
const setupRoutes = () => {
    // Health check route (before other middleware)
    app.get('/health', (req, res) => {
        res.status(200).json({
            status: 'OK',
            timestamp: new Date().toISOString(),
            environment: config.env,
            version: config.app.version,
            uptime: process.uptime()
        });
    });

    // API routes
    app.use('/api', routes);

    // Root route
    app.get('/', (req, res) => {
        res.render('index', {
            title: config.app.name,
            version: config.app.version,
            env: config.env
        });
    });

    // 404 handler
    app.use(notFoundHandler);

    // Validation error handler
    app.use(validationErrorHandler);

    // Global error handler (must be last)
    app.use(errorHandler);

    debugApp('Routes setup completed');
};

// Database connections
const connectServices = async () => {
    try {
        await connectDatabase();
        await connectRedis();
        logger.info('All services connected successfully');
    } catch (error) {
        logger.error('Failed to connect services:', error);
        process.exit(1);
    }
};

// Graceful shutdown handler
const setupGracefulShutdown = () => {
    const gracefulShutdown = (signal) => {
        logger.info(`${signal} received, shutting down gracefully`);

        process.exit(0);
    };

    process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
    process.on('SIGINT', () => gracefulShutdown('SIGINT'));

    process.on('uncaughtException', (error) => {
        logger.error('Uncaught Exception:', error);
        process.exit(1);
    });

    process.on('unhandledRejection', (reason, promise) => {
        logger.error('Unhandled Rejection at:', promise, 'reason:', reason);
        process.exit(1);
    });
};

// Application initialization
const initializeApp = async () => {
    try {
        debugApp('Initializing application...');

        await ensureDirectories();
        await connectServices();
        setupMiddleware();
        setupRoutes();
        setupGracefulShutdown();

        logger.info(`${config.app.name} v${config.app.version} initialized successfully`);
        debugApp('Application initialization completed');

        return app;
    } catch (error) {
        logger.error('Failed to initialize application:', error);
        process.exit(1);
    }
};

// Export the app for testing or direct use
export default app;

// Initialize if running directly
if (import.meta.url === `file://${process.argv[1]}`) {
    initializeApp().then(async (app) => {
        const server = await import('./server.js');
        server.startServer(app);
    }).catch(error => {
        logger.error('Failed to initialize application:', error);
        process.exit(1);
    });
}