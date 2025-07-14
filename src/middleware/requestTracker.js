import clsRTracker from 'cls-rtracer';
import { v4 as uuidv4 } from 'uuid';
import debug from 'debug';

const debugTracker = debug('app:tracker');

// Initialize CLS namespace
const ns = clsRTracker.createNamespace();

/**
 * Request tracking middleware using cls-rtracker
 */
export const requestTracker = (req, res, next) => {
    const requestId = req.headers['x-request-id'] || uuidv4();

    // Set request ID in headers
    req.requestId = requestId;
    res.setHeader('X-Request-ID', requestId);

    // Run in CLS context
    ns.run(() => {
        ns.set('requestId', requestId);
        ns.set('startTime', Date.now());
        ns.set('userId', null); // Will be set after authentication

        debugTracker(`Request ${requestId} started: ${req.method} ${req.originalUrl}`);

        // Log request completion
        res.on('finish', () => {
            const duration = Date.now() - ns.get('startTime');
            debugTracker(`Request ${requestId} completed: ${res.statusCode} (${duration}ms)`);
        });

        next();
    });
};

/**
 * Get current request ID from CLS
 */
export const getRequestId = () => {
    return ns.get('requestId');
};

/**
 * Get current user ID from CLS
 */
export const getUserId = () => {
    return ns.get('userId');
};

/**
 * Set user ID in CLS context
 */
export const setUserId = (userId) => {
    ns.set('userId', userId);
};
