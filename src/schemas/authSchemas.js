import Joi from 'joi';
import debug from 'debug';

const debugAuthSchemas = debug('app:authSchemas');

debugAuthSchemas('Loading authSchemas');

/**
 * Password validation schema
 */
const passwordSchema = Joi.string()
    .min(8)
    .max(128)
    .pattern(/^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]/)
    .required()
    .messages({
        'string.pattern.base': 'Password must contain at least one uppercase letter, one lowercase letter, one number, and one special character',
        'string.min': 'Password must be at least 8 characters long',
        'string.max': 'Password must not exceed 128 characters'
    });

/**
 * Email validation schema
 */
const emailSchema = Joi.string()
    .email({ tlds: { allow: false } })
    .max(254)
    .required()
    .messages({
        'string.email': 'Please provide a valid email address',
        'string.max': 'Email address is too long'
    });

/**
 * User registration schema
 */
export const registerSchema = Joi.object({
    firstName: Joi.string()
        .trim()
        .min(2)
        .max(50)
        .pattern(/^[a-zA-Z\s]+$/)
        .required()
        .messages({
            'string.pattern.base': 'First name can only contain letters and spaces',
            'string.min': 'First name must be at least 2 characters long',
            'string.max': 'First name must not exceed 50 characters'
        }),

    lastName: Joi.string()
        .trim()
        .min(2)
        .max(50)
        .pattern(/^[a-zA-Z\s]+$/)
        .required()
        .messages({
            'string.pattern.base': 'Last name can only contain letters and spaces',
            'string.min': 'Last name must be at least 2 characters long',
            'string.max': 'Last name must not exceed 50 characters'
        }),

    email: emailSchema,
    password: passwordSchema,

    confirmPassword: Joi.string()
        .valid(Joi.ref('password'))
        .required()
        .messages({
            'any.only': 'Password confirmation does not match'
        }),

    phone: Joi.string()
        .pattern(/^\+?[1-9]\d{1,14}$/)
        .optional()
        .messages({
            'string.pattern.base': 'Please provide a valid phone number'
        }),

    dateOfBirth: Joi.date()
        .max('now')
        .iso()
        .optional()
        .messages({
            'date.max': 'Date of birth cannot be in the future'
        }),

    terms: Joi.boolean()
        .valid(true)
        .required()
        .messages({
            'any.only': 'You must accept the terms and conditions'
        })
}).options({ stripUnknown: true });

/**
 * User login schema
 */
export const loginSchema = Joi.object({
    email: emailSchema,
    password: Joi.string().required().messages({
        'string.empty': 'Password is required'
    }),
    rememberMe: Joi.boolean().optional().default(false)
}).options({ stripUnknown: true });

/**
 * Password reset request schema
 */
export const forgotPasswordSchema = Joi.object({
    email: emailSchema
}).options({ stripUnknown: true });

/**
 * Password reset schema
 */
export const resetPasswordSchema = Joi.object({
    token: Joi.string()
        .length(64)
        .hex()
        .required()
        .messages({
            'string.length': 'Invalid reset token',
            'string.hex': 'Invalid reset token format'
        }),

    password: passwordSchema,

    confirmPassword: Joi.string()
        .valid(Joi.ref('password'))
        .required()
        .messages({
            'any.only': 'Password confirmation does not match'
        })
}).options({ stripUnknown: true });

/**
 * Change password schema
 */
export const changePasswordSchema = Joi.object({
    currentPassword: Joi.string().required().messages({
        'string.empty': 'Current password is required'
    }),
    newPassword: passwordSchema,
    confirmPassword: Joi.string()
        .valid(Joi.ref('newPassword'))
        .required()
        .messages({
            'any.only': 'Password confirmation does not match'
        })
}).options({ stripUnknown: true });

/**
 * Refresh token schema
 */
export const refreshTokenSchema = Joi.object({
    refreshToken: Joi.string().required().messages({
        'string.empty': 'Refresh token is required'
    })
}).options({ stripUnknown: true });
