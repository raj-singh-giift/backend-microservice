import jwt from 'jsonwebtoken';
import config from '../config/index.js';
import logger from '../config/logger.js';
import { getRequestId, setUserId } from './requestTracker.js';

const debugAuth = debug('app:auth');

/**
 * JWT Authentication middleware
 */
export const authenticateToken = (options = {}) => {
    const { required = true, roles = [] } = options;

    return async (req, res, next) => {
        try {
            const authHeader = req.headers.authorization;
            const token = authHeader && authHeader.split(' ')[1];

            if (!token) {
                if (!required) {
                    return next();
                }

                logger.warn('Authentication failed: No token provided', {
                    requestId: getRequestId(),
                    ip: req.ip,
                    userAgent: req.get('User-Agent')
                });

                return res.status(401).json({
                    error: 'Access denied',
                    message: 'No token provided'
                });
            }

            // Verify JWT token
            const decoded = jwt.verify(token, config.jwt.secret, {
                issuer: config.jwt.issuer,
                audience: config.jwt.audience
            });

            // Check if token is blacklisted (you might want to implement this)
            // const isBlacklisted = await checkTokenBlacklist(token);
            // if (isBlacklisted) {
            //   throw new Error('Token is blacklisted');
            // }

            // Check role authorization
            if (roles.length > 0 && !roles.includes(decoded.role)) {
                logger.warn('Authorization failed: Insufficient permissions', {
                    requestId: getRequestId(),
                    userId: decoded.userId,
                    requiredRoles: roles,
                    userRole: decoded.role
                });

                return res.status(403).json({
                    error: 'Forbidden',
                    message: 'Insufficient permissions'
                });
            }

            // Set user information in request and CLS
            req.user = {
                id: decoded.userId,
                email: decoded.email,
                role: decoded.role,
                permissions: decoded.permissions || []
            };

            setUserId(decoded.userId);

            debugAuth(`User authenticated: ${decoded.email} (${decoded.userId})`);
            next();

        } catch (error) {
            logger.warn('Authentication failed:', {
                error: error.message,
                requestId: getRequestId(),
                ip: req.ip
            });

            const message = error.name === 'TokenExpiredError'
                ? 'Token expired'
                : 'Invalid token';

            return res.status(401).json({
                error: 'Authentication failed',
                message
            });
        }
    };
};

/**
 * Generate JWT token
 */
export const generateToken = (payload) => {
    return jwt.sign(
        {
            userId: payload.id,
            email: payload.email,
            role: payload.role,
            permissions: payload.permissions
        },
        config.jwt.secret,
        {
            expiresIn: config.jwt.expiresIn,
            issuer: config.jwt.issuer,
            audience: config.jwt.audience
        }
    );
};

/**
 * Generate refresh token
 */
export const generateRefreshToken = (payload) => {
    return jwt.sign(
        {
            userId: payload.id,
            tokenType: 'refresh'
        },
        config.jwt.refreshSecret,
        {
            expiresIn: config.jwt.refreshExpiresIn,
            issuer: config.jwt.issuer,
            audience: config.jwt.audience
        }
    );
};