import logger from '../config/logger.js';
import config from '../config/index.js';
import { getRequestId } from './requestTracker.js';
import debug from 'debug';

const debugErrorHandler = debug('app:errorHandler');

debugErrorHandler('Loading errorHandler middleware');

/**
 * Custom error class for application errors
 */
export class AppError extends Error {
    constructor (message, statusCode = 500, isOperational = true) {
        super(message);
        this.statusCode = statusCode;
        this.isOperational = isOperational;
        this.name = this.constructor.name;

        Error.captureStackTrace(this, this.constructor);
    }
}

/**
 * 404 Not Found handler
 */
export const notFoundHandler = (req, res, next) => {
    const error = new AppError(`Route ${req.originalUrl} not found`, 404);

    logger.warn('Route not found:', {
        method: req.method,
        url: req.originalUrl,
        ip: req.ip,
        userAgent: req.get('User-Agent'),
        requestId: getRequestId()
    });

    next(error);
};

/**
 * Global error handler
 */
export const errorHandler = (error, req, res, next) => {
    let { statusCode = 500, message } = error;

    // Log error details
    const errorContext = {
        error: {
            name: error.name,
            message: error.message,
            stack: error.stack
        },
        request: {
            method: req.method,
            url: req.originalUrl,
            headers: req.headers,
            body: req.body,
            params: req.params,
            query: req.query,
            ip: req.ip,
            userAgent: req.get('User-Agent')
        },
        user: req.user || null,
        requestId: getRequestId()
    };

    // Different logging levels based on error type
    if (statusCode >= 500) {
        logger.error('Server error:', errorContext);
    } else if (statusCode >= 400) {
        logger.warn('Client error:', errorContext);
    } else {
        logger.info('Request error:', errorContext);
    }

    // Handle specific error types
    if (error.name === 'ValidationError') {
        statusCode = 422;
        message = 'Validation failed';
    } else if (error.name === 'UnauthorizedError') {
        statusCode = 401;
        message = 'Unauthorized';
    } else if (error.name === 'CastError') {
        statusCode = 400;
        message = 'Invalid ID format';
    } else if (error.code === 11000) {
        statusCode = 409;
        message = 'Duplicate entry';
    } else if (error.name === 'JsonWebTokenError') {
        statusCode = 401;
        message = 'Invalid token';
    } else if (error.name === 'TokenExpiredError') {
        statusCode = 401;
        message = 'Token expired';
    }

    // Don't leak error details in production
    const response = {
        error: true,
        message: message,
        requestId: getRequestId(),
        timestamp: new Date().toISOString()
    };

    // Include stack trace in development
    if (config.env === 'development') {
        response.stack = error.stack;
        response.details = error;
    }

    // Send error response
    res.status(statusCode).json(response);
};

/**
 * Async error wrapper to catch async errors
 */
export const asyncHandler = (fn) => {
    return (req, res, next) => {
        Promise.resolve(fn(req, res, next)).catch(next);
    };
};