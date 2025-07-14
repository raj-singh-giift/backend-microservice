export const ERROR_MESSAGES = {
    // Authentication
    INVALID_CREDENTIALS: 'Invalid email or password',
    ACCOUNT_NOT_VERIFIED: 'Please verify your email address before logging in',
    ACCOUNT_SUSPENDED: 'Your account has been suspended',
    TOKEN_EXPIRED: 'Your session has expired. Please login again',
    INVALID_TOKEN: 'Invalid authentication token',

    // Authorization
    INSUFFICIENT_PERMISSIONS: 'You do not have permission to perform this action',
    ADMIN_REQUIRED: 'Administrator privileges required',

    // Validation
    REQUIRED_FIELD: 'This field is required',
    INVALID_EMAIL: 'Please provide a valid email address',
    INVALID_PASSWORD: 'Password must contain at least 8 characters with uppercase, lowercase, number and special character',
    PASSWORD_MISMATCH: 'Passwords do not match',

    // User Management
    USER_NOT_FOUND: 'User not found',
    EMAIL_ALREADY_EXISTS: 'An account with this email already exists',
    CANNOT_DELETE_SELF: 'You cannot delete your own account',

    // General
    SERVER_ERROR: 'An internal server error occurred',
    NOT_FOUND: 'The requested resource was not found',
    BAD_REQUEST: 'Invalid request data',
    RATE_LIMIT_EXCEEDED: 'Too many requests. Please try again later',
    SERVICE_UNAVAILABLE: 'Service temporarily unavailable'
};