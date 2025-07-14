import dotenv from 'dotenv';
import path from 'path';
import debug from 'debug';

// Load environment variables
dotenv.config();

const debugConfig = debug('app:config');

/**
 * Validate required environment variables
 */
const validateEnvVars = () => {
    const required = [
        'JWT_SECRET',
        'DB_HOST',
        'DB_NAME',
        'DB_USER',
        'DB_PASSWORD'
    ];

    const missing = required.filter(key => !process.env[key]);

    if (missing.length > 0) {
        throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
    }
};

// Validate on startup
validateEnvVars();

const config = {
    // Application
    app: {
        name: process.env.APP_NAME || 'Production Backend API',
        version: process.env.APP_VERSION || '1.0.0'
    },

    // Environment
    env: process.env.NODE_ENV || 'development',
    port: parseInt(process.env.PORT, 10) || 3000,

    // SSL/HTTPS
    useHttps: process.env.USE_HTTPS === 'true',
    ssl: {
        certPath: process.env.SSL_CERT_PATH || './certs/cert.pem',
        keyPath: process.env.SSL_KEY_PATH || './certs/key.pem'
    },

    // Database
    database: {
        host: process.env.DB_HOST,
        port: parseInt(process.env.DB_PORT, 10) || 5432,
        name: process.env.DB_NAME,
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        ssl: process.env.DB_SSL === 'true',
        pool: {
            min: parseInt(process.env.DB_POOL_MIN, 10) || 2,
            max: parseInt(process.env.DB_POOL_MAX, 10) || 10
        },
        connectionTimeoutMillis: 30000,
        idleTimeoutMillis: 30000,
        query_timeout: 30000
    },

    // Redis
    redis: {
        host: process.env.REDIS_HOST || 'localhost',
        port: parseInt(process.env.REDIS_PORT, 10) || 6379,
        password: process.env.REDIS_PASSWORD || undefined,
        db: parseInt(process.env.REDIS_DB, 10) || 0,
        ttl: parseInt(process.env.REDIS_TTL, 10) || 3600,
        retryDelayOnFailover: 100,
        enableReadyCheck: false,
        maxRetriesPerRequest: 3
    },

    // JWT
    jwt: {
        secret: process.env.JWT_SECRET,
        refreshSecret: process.env.JWT_REFRESH_SECRET || process.env.JWT_SECRET,
        expiresIn: process.env.JWT_EXPIRE_TIME || '1h',
        refreshExpiresIn: process.env.JWT_REFRESH_EXPIRE_TIME || '7d',
        issuer: process.env.JWT_ISSUER || 'production-backend-api',
        audience: process.env.JWT_AUDIENCE || 'production-backend-users'
    },

    // Logging
    logging: {
        level: process.env.LOG_LEVEL || 'info',
        fileEnabled: process.env.LOG_FILE_ENABLED === 'true',
        maxSize: process.env.LOG_MAX_SIZE || '20m',
        maxFiles: process.env.LOG_MAX_FILES || '14d',
        format: process.env.LOG_FORMAT || 'json'
    },

    // Rate Limiting
    rateLimit: {
        windowMs: parseInt(process.env.RATE_LIMIT_WINDOW_MS, 10) || 15 * 60 * 1000, // 15 minutes
        maxRequests: parseInt(process.env.RATE_LIMIT_MAX_REQUESTS, 10) || 100,
        message: 'Too many requests from this IP, please try again later',
        standardHeaders: true,
        legacyHeaders: false
    },

    // CORS
    cors: {
        origins: process.env.CORS_ORIGINS ? process.env.CORS_ORIGINS.split(',') : ['http://localhost:3000'],
        credentials: process.env.CORS_CREDENTIALS === 'true'
    },

    // Security
    security: {
        bcryptSaltRounds: parseInt(process.env.BCRYPT_SALT_ROUNDS, 10) || 12,
        cookieSecret: process.env.CSRF_SECRET || 'your-cookie-secret',
        sessionSecret: process.env.SESSION_SECRET || 'your-session-secret'
    },

    // File Upload
    upload: {
        maxFileSize: parseInt(process.env.MAX_FILE_SIZE, 10) || 10 * 1024 * 1024, // 10MB
        allowedTypes: process.env.ALLOWED_FILE_TYPES ? process.env.ALLOWED_FILE_TYPES.split(',') : ['image/jpeg', 'image/png', 'image/gif'],
        destination: process.env.UPLOAD_DESTINATION || './uploads'
    }
};

debugConfig('Configuration loaded:', {
    env: config.env,
    port: config.port,
    useHttps: config.useHttps,
    database: { ...config.database, password: '[HIDDEN]' },
    redis: { ...config.redis, password: '[HIDDEN]' }
});

export default config;
