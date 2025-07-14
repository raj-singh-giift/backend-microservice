import winston from 'winston';
import DailyRotateFile from 'winston-daily-rotate-file';
import path from 'path';
import config from './index.js';

const { combine, timestamp, errors, json, printf, colorize, align } = winston.format;

// Custom format for console output in development
const consoleFormat = printf(({ level, message, timestamp, service, requestId, ...meta }) => {
    let log = `[${timestamp}] ${level}: ${message}`;

    if (requestId) {
        log = `[${timestamp}] [${requestId}] ${level}: ${message}`;
    }

    if (service) {
        log = `[${timestamp}] [${service}] ${level}: ${message}`;
    }

    if (Object.keys(meta).length > 0) {
        log += ` ${JSON.stringify(meta)}`;
    }

    return log;
});

// Create transports array
const transports = [];

// Console transport
if (config.env !== 'test') {
    transports.push(
        new winston.transports.Console({
            level: config.logging.level,
            format: combine(
                errors({ stack: true }),
                timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
                config.env === 'production' ? json() : combine(
                    colorize({ all: true }),
                    align(),
                    consoleFormat
                )
            )
        })
    );
}

// File transport for production
if (config.logging.fileEnabled) {
    // Combined logs
    transports.push(
        new DailyRotateFile({
            filename: path.join('logs', 'combined-%DATE%.log'),
            datePattern: 'YYYY-MM-DD',
            maxSize: config.logging.maxSize,
            maxFiles: config.logging.maxFiles,
            level: config.logging.level,
            format: combine(
                errors({ stack: true }),
                timestamp(),
                json()
            )
        })
    );

    // Error logs
    transports.push(
        new DailyRotateFile({
            filename: path.join('logs', 'error-%DATE%.log'),
            datePattern: 'YYYY-MM-DD',
            maxSize: config.logging.maxSize,
            maxFiles: config.logging.maxFiles,
            level: 'error',
            format: combine(
                errors({ stack: true }),
                timestamp(),
                json()
            )
        })
    );

    // HTTP logs
    transports.push(
        new DailyRotateFile({
            filename: path.join('logs', 'http-%DATE%.log'),
            datePattern: 'YYYY-MM-DD',
            maxSize: config.logging.maxSize,
            maxFiles: config.logging.maxFiles,
            level: 'info',
            format: combine(
                timestamp(),
                json()
            ),
            // Only log HTTP-related messages
            filter: (info) => info.service === 'http'
        })
    );
}

// Create logger instance
const logger = winston.createLogger({
    level: config.logging.level,
    levels: winston.config.npm.levels,
    format: combine(
        errors({ stack: true }),
        timestamp(),
        json()
    ),
    transports,
    exitOnError: false,
    silent: config.env === 'test'
});

// Handle uncaught exceptions and rejections
if (config.logging.fileEnabled) {
    logger.exceptions.handle(
        new DailyRotateFile({
            filename: path.join('logs', 'exceptions-%DATE%.log'),
            datePattern: 'YYYY-MM-DD',
            maxSize: config.logging.maxSize,
            maxFiles: config.logging.maxFiles,
            format: combine(
                errors({ stack: true }),
                timestamp(),
                json()
            )
        })
    );

    logger.rejections.handle(
        new DailyRotateFile({
            filename: path.join('logs', 'rejections-%DATE%.log'),
            datePattern: 'YYYY-MM-DD',
            maxSize: config.logging.maxSize,
            maxFiles: config.logging.maxFiles,
            format: combine(
                errors({ stack: true }),
                timestamp(),
                json()
            )
        })
    );
}

// Create child logger function for adding context
logger.child = (defaultMeta = {}) => {
    return {
        error: (message, meta = {}) => logger.error(message, { ...defaultMeta, ...meta }),
        warn: (message, meta = {}) => logger.warn(message, { ...defaultMeta, ...meta }),
        info: (message, meta = {}) => logger.info(message, { ...defaultMeta, ...meta }),
        debug: (message, meta = {}) => logger.debug(message, { ...defaultMeta, ...meta }),
        verbose: (message, meta = {}) => logger.verbose(message, { ...defaultMeta, ...meta })
    };
};

export default logger;