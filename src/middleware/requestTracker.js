import cls from 'cls-hooked';
import { v4 as uuidv4 } from 'uuid';
import debug from 'debug';

const debugTracker = debug('app:tracker');

// Create CLS namespace
const namespace = cls.createNamespace('request-context');

/**
 * Request tracking middleware using cls-hooked
 */
export const requestTracker = (req, res, next) => {
    const requestId = req.headers['x-request-id'] || uuidv4();

    // Set request ID in headers
    req.requestId = requestId;
    res.setHeader('X-Request-ID', requestId);

    // Run in CLS context
    namespace.run(() => {
        namespace.set('requestId', requestId);
        namespace.set('startTime', Date.now());
        namespace.set('userId', null); // Will be set after authentication

        debugTracker(`Request ${requestId} started: ${req.method} ${req.originalUrl}`);

        // Log request completion
        res.on('finish', () => {
            const duration = Date.now() - namespace.get('startTime');
            debugTracker(`Request ${requestId} completed: ${res.statusCode} (${duration}ms)`);
        });

        next();
    });
};

/**
 * Get current request ID from CLS
 */
export const getRequestId = () => {
    if (namespace && namespace.active) {
        return namespace.get('requestId');
    }
    return null;
};

/**
 * Get current user ID from CLS
 */
export const getUserId = () => {
    if (namespace && namespace.active) {
        return namespace.get('userId');
    }
    return null;
};

/**
 * Set user ID in CLS context
 */
export const setUserId = (userId) => {
    if (namespace && namespace.active) {
        namespace.set('userId', userId);
    }
};