import crypto from 'crypto';
import bcrypt from 'bcrypt';
import config from '../config/index.js';

/**
 * Hash password using bcrypt
 * @param {string} password - Plain text password
 * @returns {Promise<string>} Hashed password
 */
export const hashPassword = async (password) => {
    return await bcrypt.hash(password, config.security.bcryptSaltRounds);
};

/**
 * Compare password with hash
 * @param {string} password - Plain text password
 * @param {string} hash - Hashed password
 * @returns {Promise<boolean>} Comparison result
 */
export const comparePassword = async (password, hash) => {
    return await bcrypt.compare(password, hash);
};

/**
 * Generate random token
 * @param {number} length - Token length in bytes
 * @returns {string} Random token
 */
export const generateRandomToken = (length = 32) => {
    return crypto.randomBytes(length).toString('hex');
};

/**
 * Generate UUID v4
 * @returns {string} UUID
 */
export const generateUUID = () => {
    return crypto.randomUUID();
};

/**
 * Create HMAC signature
 * @param {string} data - Data to sign
 * @param {string} secret - Secret key
 * @param {string} algorithm - Hash algorithm
 * @returns {string} HMAC signature
 */
export const createHMAC = (data, secret, algorithm = 'sha256') => {
    return crypto.createHmac(algorithm, secret).update(data).digest('hex');
};

/**
 * Verify HMAC signature
 * @param {string} data - Original data
 * @param {string} signature - Signature to verify
 * @param {string} secret - Secret key
 * @param {string} algorithm - Hash algorithm
 * @returns {boolean} Verification result
 */
export const verifyHMAC = (data, signature, secret, algorithm = 'sha256') => {
    const expectedSignature = createHMAC(data, secret, algorithm);
    return crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(expectedSignature));
};

/**
 * Encrypt data using AES-256-GCM
 * @param {string} text - Text to encrypt
 * @param {string} key - Encryption key (32 bytes)
 * @returns {Object} Encrypted data with IV and auth tag
 */
export const encryptData = (text, key) => {
    const algorithm = 'aes-256-gcm';
    const iv = crypto.randomBytes(16);
    const cipher = crypto.createCipher(algorithm, key);
    cipher.setAAD(Buffer.from('additional-data'));

    let encrypted = cipher.update(text, 'utf8', 'hex');
    encrypted += cipher.final('hex');

    const authTag = cipher.getAuthTag();

    return {
        encrypted,
        iv: iv.toString('hex'),
        authTag: authTag.toString('hex')
    };
};

/**
 * Decrypt data using AES-256-GCM
 * @param {Object} encryptedData - Encrypted data object
 * @param {string} key - Decryption key
 * @returns {string} Decrypted text
 */
export const decryptData = (encryptedData, key) => {
    const algorithm = 'aes-256-gcm';
    const decipher = crypto.createDecipher(algorithm, key);

    decipher.setAAD(Buffer.from('additional-data'));
    decipher.setAuthTag(Buffer.from(encryptedData.authTag, 'hex'));

    let decrypted = decipher.update(encryptedData.encrypted, 'hex', 'utf8');
    decrypted += decipher.final('utf8');

    return decrypted;
};