import axios from 'axios';
import config from '../config/index.js';
import logger from '../config/logger.js';
import { getRequestId } from '../middleware/requestTracker.js';

/**
 * Create configured axios instance
 */
const createHttpClient = (baseConfig = {}) => {
    const client = axios.create({
        timeout: 30000,
        headers: {
            'User-Agent': `${config.app.name}/${config.app.version}`,
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        },
        ...baseConfig
    });

    // Request interceptor
    client.interceptors.request.use(
        (config) => {
            const requestId = getRequestId();
            config.metadata = { startTime: Date.now(), requestId };

            logger.info('HTTP request started:', {
                method: config.method?.toUpperCase(),
                url: config.url,
                baseURL: config.baseURL,
                requestId,
                service: 'http-client'
            });

            return config;
        },
        (error) => {
            logger.error('HTTP request setup failed:', error);
            return Promise.reject(error);
        }
    );

    // Response interceptor
    client.interceptors.response.use(
        (response) => {
            const duration = Date.now() - response.config.metadata.startTime;

            logger.info('HTTP request completed:', {
                method: response.config.method?.toUpperCase(),
                url: response.config.url,
                status: response.status,
                duration: `${duration}ms`,
                requestId: response.config.metadata.requestId,
                service: 'http-client'
            });

            return response;
        },
        (error) => {
            const duration = error.config?.metadata ?
                Date.now() - error.config.metadata.startTime : 0;

            logger.error('HTTP request failed:', {
                method: error.config?.method?.toUpperCase(),
                url: error.config?.url,
                status: error.response?.status,
                duration: `${duration}ms`,
                error: error.message,
                requestId: error.config?.metadata?.requestId,
                service: 'http-client'
            });

            return Promise.reject(error);
        }
    );

    return client;
};

/**
 * Default HTTP client instance
 */
export const httpClient = createHttpClient();

/**
 * Create HTTP client with authentication
 * @param {string} token - Authentication token
 * @param {string} type - Token type (Bearer, Basic, etc.)
 * @returns {Object} Axios instance with auth
 */
export const createAuthenticatedClient = (token, type = 'Bearer') => {
    return createHttpClient({
        headers: {
            'Authorization': `${type} ${token}`
        }
    });
};

/**
 * Generic GET request with caching
 * @param {string} url - Request URL
 * @param {Object} options - Request options
 * @returns {Promise<Object>} Response data
 */
export const get = async (url, options = {}) => {
    const { client = httpClient, cache = false, ...config } = options;

    try {
        const response = await client.get(url, config);
        return response.data;
    } catch (error) {
        throw new Error(`GET request failed: ${error.message}`);
    }
};

/**
 * Generic POST request
 * @param {string} url - Request URL
 * @param {Object} data - Request data
 * @param {Object} options - Request options
 * @returns {Promise<Object>} Response data
 */
export const post = async (url, data = {}, options = {}) => {
    const { client = httpClient, ...config } = options;

    try {
        const response = await client.post(url, data, config);
        return response.data;
    } catch (error) {
        throw new Error(`POST request failed: ${error.message}`);
    }
};

/**
 * Generic PUT request
 * @param {string} url - Request URL
 * @param {Object} data - Request data
 * @param {Object} options - Request options
 * @returns {Promise<Object>} Response data
 */
export const put = async (url, data = {}, options = {}) => {
    const { client = httpClient, ...config } = options;

    try {
        const response = await client.put(url, data, config);
        return response.data;
    } catch (error) {
        throw new Error(`PUT request failed: ${error.message}`);
    }
};

/**
 * Generic DELETE request
 * @param {string} url - Request URL
 * @param {Object} options - Request options
 * @returns {Promise<Object>} Response data
 */
export const del = async (url, options = {}) => {
    const { client = httpClient, ...config } = options;

    try {
        const response = await client.delete(url, config);
        return response.data;
    } catch (error) {
        throw new Error(`DELETE request failed: ${error.message}`);
    }
};