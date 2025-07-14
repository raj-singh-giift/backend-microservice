import debug from 'debug';
import config from '../config/index.js';
import logger from '../config/logger.js';
import { getRequestId } from './requestTracker.js';

const debugSecurity = debug('app:security');

/**
 * Security headers and protection middleware
 */
export const securityMiddleware = [
    // Additional security headers beyond helmet
    (req, res, next) => {
        // Prevent MIME type sniffing
        res.setHeader('X-Content-Type-Options', 'nosniff');

        // Prevent clickjacking
        res.setHeader('X-Frame-Options', 'DENY');

        // XSS protection
        res.setHeader('X-XSS-Protection', '1; mode=block');

        // Referrer policy
        res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');

        // Feature policy
        res.setHeader('Permissions-Policy', 'geolocation=(), microphone=(), camera=()');

        debugSecurity('Security headers applied');
        next();
    },

    // Request size limiting
    (req, res, next) => {
        const contentLength = parseInt(req.get('content-length'));

        if (contentLength && contentLength > 10 * 1024 * 1024) { // 10MB limit
            logger.warn('Request too large:', {
                contentLength,
                requestId: getRequestId(),
                ip: req.ip
            });

            return res.status(413).json({
                error: 'Payload too large',
                message: 'Request size exceeds limit'
            });
        }

        next();
    },

    // IP validation and blocking
    (req, res, next) => {
        const clientIP = req.ip;

        // Add your IP blocking logic here
        // const blockedIPs = ['1.2.3.4', '5.6.7.8'];
        // if (blockedIPs.includes(clientIP)) {
        //   logger.warn('Blocked IP attempt:', { ip: clientIP, requestId: getRequestId() });
        //   return res.status(403).json({ error: 'Access denied' });
        // }

        debugSecurity(`Request from IP: ${clientIP}`);
        next();
    }
];