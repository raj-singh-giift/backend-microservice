import validator from 'validator';

/**
 * Sanitize and validate email
 * @param {string} email - Email to sanitize
 * @returns {string|null} Sanitized email or null if invalid
 */
export const sanitizeEmail = (email) => {
    if (!email || typeof email !== 'string') return null;

    const sanitized = validator.normalizeEmail(email.toLowerCase().trim());
    return validator.isEmail(sanitized) ? sanitized : null;
};

/**
 * Sanitize string input
 * @param {string} input - String to sanitize
 * @param {Object} options - Sanitization options
 * @returns {string} Sanitized string
 */
export const sanitizeString = (input, options = {}) => {
    if (!input || typeof input !== 'string') return '';

    const {
        maxLength = 255,
        allowHTML = false,
        trim = true
    } = options;

    let sanitized = input;

    if (trim) {
        sanitized = sanitized.trim();
    }

    if (!allowHTML) {
        sanitized = validator.escape(sanitized);
    }

    if (maxLength > 0) {
        sanitized = sanitized.substring(0, maxLength);
    }

    return sanitized;
};

/**
 * Sanitize phone number
 * @param {string} phone - Phone number to sanitize
 * @param {string} locale - Locale for validation
 * @returns {string|null} Sanitized phone or null if invalid
 */
export const sanitizePhone = (phone, locale = 'en-US') => {
    if (!phone || typeof phone !== 'string') return null;

    // Remove all non-digit characters except +
    const cleaned = phone.replace(/[^\d+]/g, '');

    return validator.isMobilePhone(cleaned, locale) ? cleaned : null;
};

/**
 * Sanitize URL
 * @param {string} url - URL to sanitize
 * @param {Object} options - Validation options
 * @returns {string|null} Sanitized URL or null if invalid
 */
export const sanitizeURL = (url, options = {}) => {
    if (!url || typeof url !== 'string') return null;

    const {
        protocols = ['http', 'https'],
        requireProtocol = true
    } = options;

    const sanitized = url.trim();

    return validator.isURL(sanitized, {
        protocols,
        require_protocol: requireProtocol
    }) ? sanitized : null;
};

/**
 * Remove dangerous characters for SQL injection prevention
 * @param {string} input - Input to sanitize
 * @returns {string} Sanitized input
 */
export const sanitizeForSQL = (input) => {
    if (!input || typeof input !== 'string') return '';

    // Remove potentially dangerous characters
    return input.replace(/['";\\]/g, '');
};

/**
 * Sanitize filename for file uploads
 * @param {string} filename - Filename to sanitize
 * @returns {string} Sanitized filename
 */
export const sanitizeFilename = (filename) => {
    if (!filename || typeof filename !== 'string') return '';

    // Remove path traversal attempts and dangerous characters
    return filename
        .replace(/[<>:"/\\|?*\x00-\x1f]/g, '')
        .replace(/^\.+/, '')
        .substring(0, 255);
};