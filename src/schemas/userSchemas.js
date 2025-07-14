import Joi from 'joi';

/**
 * User profile update schema
 */
export const updateProfileSchema = Joi.object({
    firstName: Joi.string()
        .trim()
        .min(2)
        .max(50)
        .pattern(/^[a-zA-Z\s]+$/)
        .optional(),

    lastName: Joi.string()
        .trim()
        .min(2)
        .max(50)
        .pattern(/^[a-zA-Z\s]+$/)
        .optional(),

    phone: Joi.string()
        .pattern(/^\+?[1-9]\d{1,14}$/)
        .optional(),

    dateOfBirth: Joi.date()
        .max('now')
        .iso()
        .optional(),

    bio: Joi.string()
        .max(500)
        .optional(),

    location: Joi.string()
        .max(100)
        .optional(),

    website: Joi.string()
        .uri()
        .optional(),

    preferences: Joi.object({
        emailNotifications: Joi.boolean().default(true),
        smsNotifications: Joi.boolean().default(false),
        theme: Joi.string().valid('light', 'dark', 'auto').default('auto'),
        language: Joi.string().valid('en', 'es', 'fr', 'de').default('en')
    }).optional()
}).min(1).messages({
    'object.min': 'At least one field must be provided for update'
}).options({ stripUnknown: true });

/**
 * User query parameters schema
 */
export const userQuerySchema = Joi.object({
    page: Joi.number().integer().min(1).default(1),
    limit: Joi.number().integer().min(1).max(100).default(10),
    search: Joi.string().max(100).optional(),
    role: Joi.string().valid('admin', 'user', 'moderator').optional(),
    status: Joi.string().valid('active', 'inactive', 'suspended').optional(),
    sortBy: Joi.string().valid('created_at', 'email', 'firstName', 'lastName').default('created_at'),
    sortOrder: Joi.string().valid('asc', 'desc').default('desc')
}).options({ stripUnknown: true });

/**
 * User ID parameter schema
 */
export const userIdSchema = Joi.object({
    id: Joi.string().uuid().required().messages({
        'string.guid': 'Invalid user ID format'
    })
});