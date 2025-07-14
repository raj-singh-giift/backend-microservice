import Joi from 'joi';
import logger from '../config/logger.js';
import { getRequestId } from './requestTracker.js';

const debugValidation = debug('app:validation');

/**
 * Create validation middleware for different request parts
 */
export const validate = (schema, property = 'body') => {
    return (req, res, next) => {
        const { error, value } = schema.validate(req[property], {
            abortEarly: false,
            allowUnknown: false,
            stripUnknown: true
        });

        if (error) {
            const validationErrors = error.details.map(detail => ({
                field: detail.path.join('.'),
                message: detail.message,
                value: detail.context?.value
            }));

            logger.warn('Validation failed:', {
                property,
                errors: validationErrors,
                requestId: getRequestId()
            });

            debugValidation(`Validation failed for ${property}:`, validationErrors);

            return res.status(422).json({
                error: 'Validation failed',
                message: 'Invalid input data',
                details: validationErrors
            });
        }

        // Replace request property with validated and sanitized value
        req[property] = value;
        debugValidation(`Validation passed for ${property}`);
        next();
    };
};

/**
 * Validation error handler for global error handling
 */
export const validationErrorHandler = (error, req, res, next) => {
    if (error.isJoi) {
        const validationErrors = error.details.map(detail => ({
            field: detail.path.join('.'),
            message: detail.message,
            value: detail.context?.value
        }));

        logger.warn('Global validation error:', {
            errors: validationErrors,
            requestId: getRequestId()
        });

        return res.status(422).json({
            error: 'Validation failed',
            message: 'Invalid input data',
            details: validationErrors
        });
    }

    next(error);
};