import { Router } from 'express';
import fs from 'fs/promises';
import path from 'path';
import { marked } from 'marked';
import { asyncHandler } from '../middleware/errorHandler.js';
import logger from '../config/logger.js';
import { getRequestId } from '../middleware/requestTracker.js';
import { cacheService } from '../services/cacheService.js';
import debug from 'debug';

const debugDatabaseDocs = debug('app:databaseDocs');

debugDatabaseDocs('Loading databaseDocs');

const router = Router();
const debugDocs = debug('app:docs');

// Configuration
const DOCS_CONFIG = {
    docPath: path.join(process.cwd(), 'docs', 'database.md'),
    cacheKey: 'docs:database',
    cacheTTL: 3600, // 1 hour
    maxFileSize: 5 * 1024 * 1024, // 5MB
};

/**
 * Configure marked with modern API and security
 */
const configureMarked = () => {
    // Custom renderer using the new API
    const renderer = {
        heading({ tokens, depth }) {
            const text = this.parser.parseInline(tokens);
            // Create slug from text - safe and URL-friendly
            const slug = text
                .toLowerCase()
                .trim()
                .replace(/\s+/g, '-')
                .replace(/[^\w\-]+/g, '')
                .replace(/\-\-+/g, '-')
                .replace(/^-+/, '')
                .replace(/-+$/, '');

            return `<h${depth} id="${slug}">
                <a href="#${slug}" class="heading-link" aria-label="Link to ${text}">
                    ${text}
                </a>
            </h${depth}>`;
        },

        code({ text, lang, escaped }) {
            // Enhanced code block rendering with syntax highlighting classes
            const language = lang || '';
            const escapedText = escaped ? text : this.utils.escape(text);

            return `<pre class="code-block"><code class="language-${language}">${escapedText}</code></pre>`;
        },

        link({ href, title, text }) {
            // Secure link rendering with validation
            if (!href || typeof href !== 'string') {
                return text;
            }

            // Basic URL validation and security
            const isExternal = /^https?:\/\//.test(href);
            const titleAttr = title ? ` title="${this.utils.escape(title)}"` : '';
            const targetAttr = isExternal ? ' target="_blank" rel="noopener noreferrer"' : '';

            return `<a href="${this.utils.escape(href)}"${titleAttr}${targetAttr}>${text}</a>`;
        },

        table({ header, rows }) {
            // Enhanced table rendering with responsive wrapper
            let body = '';
            for (let i = 0; i < rows.length; i++) {
                body += '<tr>' + rows[i] + '</tr>';
            }

            return `<div class="table-wrapper">
                <table class="docs-table">
                    <thead>${header}</thead>
                    <tbody>${body}</tbody>
                </table>
            </div>`;
        }
    };

    // Security and performance options
    marked.use({
        renderer,
        gfm: true,
        breaks: false,
        pedantic: false,
        sanitize: false, // We'll handle sanitization manually if needed
        smartypants: true,
        xhtml: false
    });

    debugDocs('Marked configured with custom renderer and security options');
};

/**
 * Generate table of contents from markdown content
 */
const generateTOC = (content) => {
    const headings = [];
    const tokens = marked.lexer(content);

    const walkTokens = (tokenList) => {
        for (const token of tokenList) {
            if (token.type === 'heading' && token.depth <= 3) {
                const text = token.tokens?.map(t => t.raw || t.text || '').join('') || '';
                const slug = text
                    .toLowerCase()
                    .trim()
                    .replace(/\s+/g, '-')
                    .replace(/[^\w\-]+/g, '')
                    .replace(/\-\-+/g, '-')
                    .replace(/^-+/, '')
                    .replace(/-+$/, '');

                headings.push({
                    level: token.depth,
                    text,
                    slug
                });
            }

            if (token.tokens) {
                walkTokens(token.tokens);
            }
        }
    };

    walkTokens(tokens);
    return headings;
};

/**
 * Validate markdown file
 */
const validateMarkdownFile = async (filePath) => {
    try {
        const stats = await fs.stat(filePath);

        if (!stats.isFile()) {
            throw new Error('Path is not a file');
        }

        if (stats.size > DOCS_CONFIG.maxFileSize) {
            throw new Error(`File size exceeds limit: ${stats.size} bytes`);
        }

        return { size: stats.size, lastModified: stats.mtime };
    } catch (error) {
        logger.error('Markdown file validation failed:', {
            filePath,
            error: error.message,
            requestId: getRequestId()
        });
        throw error;
    }
};

/**
 * Read and process markdown content
 */
const processMarkdownContent = async (filePath) => {
    try {
        // Validate file first
        const fileInfo = await validateMarkdownFile(filePath);

        // Read file content
        const content = await fs.readFile(filePath, 'utf8');

        if (!content.trim()) {
            throw new Error('Markdown file is empty');
        }

        debugDocs('Markdown content loaded:', {
            size: fileInfo.size,
            lastModified: fileInfo.lastModified
        });

        return { content, fileInfo };
    } catch (error) {
        logger.error('Failed to process markdown content:', {
            filePath,
            error: error.message,
            requestId: getRequestId()
        });
        throw error;
    }
};

/**
 * Generate enhanced HTML template
 */
const generateHTMLTemplate = (html, toc, title = 'Database Documentation') => {
    const tocHTML = toc.length > 0 ? `
        <nav class="table-of-contents">
            <h3>Table of Contents</h3>
            <ul>
                ${toc.map(item => `
                    <li class="toc-level-${item.level}">
                        <a href="#${item.slug}">${item.text}</a>
                    </li>
                `).join('')}
            </ul>
        </nav>
    ` : '';

    return `
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>${title}</title>
        <meta name="description" content="Database utilities and best practices documentation">
        <style>
            :root {
                --primary-color: #2c3e50;
                --secondary-color: #3498db;
                --background-color: #ffffff;
                --text-color: #333333;
                --border-color: #e1e8ed;
                --code-background: #f8f9fa;
                --shadow: 0 2px 4px rgba(0,0,0,0.1);
            }

            * {
                box-sizing: border-box;
            }

            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                line-height: 1.6;
                color: var(--text-color);
                background-color: var(--background-color);
                margin: 0;
                padding: 0;
            }

            .container {
                max-width: 1200px;
                margin: 0 auto;
                padding: 2rem;
                display: grid;
                grid-template-columns: 250px 1fr;
                gap: 2rem;
            }

            .table-of-contents {
                position: sticky;
                top: 2rem;
                background: var(--code-background);
                padding: 1.5rem;
                border-radius: 8px;
                border: 1px solid var(--border-color);
                max-height: calc(100vh - 4rem);
                overflow-y: auto;
            }

            .table-of-contents h3 {
                margin-top: 0;
                color: var(--primary-color);
                font-size: 1.1rem;
            }

            .table-of-contents ul {
                list-style: none;
                padding: 0;
                margin: 0;
            }

            .table-of-contents li {
                margin: 0.5rem 0;
            }

            .table-of-contents a {
                color: var(--text-color);
                text-decoration: none;
                font-size: 0.9rem;
                transition: color 0.2s ease;
            }

            .table-of-contents a:hover {
                color: var(--secondary-color);
            }

            .toc-level-2 { padding-left: 1rem; }
            .toc-level-3 { padding-left: 2rem; }

            .content {
                min-width: 0;
            }

            h1, h2, h3, h4, h5, h6 {
                color: var(--primary-color);
                margin-top: 2rem;
                margin-bottom: 1rem;
                font-weight: 600;
            }

            h1 { font-size: 2.5rem; margin-top: 0; }
            h2 { font-size: 2rem; border-bottom: 2px solid var(--border-color); padding-bottom: 0.5rem; }
            h3 { font-size: 1.5rem; }

            .heading-link {
                color: inherit;
                text-decoration: none;
                position: relative;
            }

            .heading-link:hover::before {
                content: '#';
                position: absolute;
                left: -1.5rem;
                color: var(--secondary-color);
                font-weight: normal;
            }

            p {
                margin-bottom: 1.5rem;
                text-align: justify;
            }

            .code-block {
                background: var(--code-background);
                border: 1px solid var(--border-color);
                border-radius: 6px;
                padding: 1rem;
                overflow-x: auto;
                margin: 1.5rem 0;
                font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
                font-size: 0.9rem;
                line-height: 1.4;
            }

            code {
                background: var(--code-background);
                padding: 0.2rem 0.4rem;
                border-radius: 3px;
                font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
                font-size: 0.9em;
            }

            .table-wrapper {
                overflow-x: auto;
                margin: 1.5rem 0;
                border: 1px solid var(--border-color);
                border-radius: 6px;
            }

            .docs-table {
                width: 100%;
                border-collapse: collapse;
                background: white;
            }

            .docs-table th,
            .docs-table td {
                padding: 0.75rem;
                text-align: left;
                border-bottom: 1px solid var(--border-color);
            }

            .docs-table th {
                background: var(--code-background);
                font-weight: 600;
                color: var(--primary-color);
            }

            .docs-table tr:hover {
                background: rgba(52, 152, 219, 0.05);
            }

            blockquote {
                border-left: 4px solid var(--secondary-color);
                margin: 1.5rem 0;
                padding: 0 1.5rem;
                color: #666;
                background: var(--code-background);
                border-radius: 0 6px 6px 0;
            }

            a {
                color: var(--secondary-color);
                text-decoration: none;
            }

            a:hover {
                text-decoration: underline;
            }

            ul, ol {
                margin-bottom: 1.5rem;
                padding-left: 2rem;
            }

            li {
                margin-bottom: 0.5rem;
            }

            @media (max-width: 768px) {
                .container {
                    grid-template-columns: 1fr;
                    padding: 1rem;
                }

                .table-of-contents {
                    position: static;
                    margin-bottom: 2rem;
                }
            }

            /* Print styles */
            @media print {
                .table-of-contents {
                    display: none;
                }
                
                .container {
                    grid-template-columns: 1fr;
                }
                
                .heading-link:hover::before {
                    display: none;
                }
            }
        </style>
    </head>
    <body>
        <div class="container">
            ${tocHTML}
            <main class="content">
                ${html}
            </main>
        </div>

        <script>
            // Smooth scrolling for anchor links
            document.querySelectorAll('a[href^="#"]').forEach(anchor => {
                anchor.addEventListener('click', function (e) {
                    e.preventDefault();
                    const target = document.querySelector(this.getAttribute('href'));
                    if (target) {
                        target.scrollIntoView({
                            behavior: 'smooth',
                            block: 'start'
                        });
                    }
                });
            });

            // Highlight current section in TOC
            const observerOptions = {
                rootMargin: '-20% 0px -35% 0px',
                threshold: 0
            };

            const observer = new IntersectionObserver((entries) => {
                entries.forEach(entry => {
                    const id = entry.target.getAttribute('id');
                    const tocLink = document.querySelector(\`.table-of-contents a[href="#\${id}"]\`);
                    
                    if (entry.isIntersecting) {
                        document.querySelectorAll('.table-of-contents a').forEach(link => {
                            link.style.fontWeight = 'normal';
                            link.style.color = 'var(--text-color)';
                        });
                        
                        if (tocLink) {
                            tocLink.style.fontWeight = '600';
                            tocLink.style.color = 'var(--secondary-color)';
                        }
                    }
                });
            }, observerOptions);

            document.querySelectorAll('h1[id], h2[id], h3[id]').forEach(heading => {
                observer.observe(heading);
            });
        </script>
    </body>
    </html>`;
};

// Initialize marked configuration
configureMarked();

/**
 * Serve documentation as HTML with enhanced features
 */
router.get('/', asyncHandler(async (req, res) => {
    const requestId = getRequestId();

    try {
        // Try to get from cache first
        let cachedData = await cacheService.get(DOCS_CONFIG.cacheKey);

        if (!cachedData) {
            debugDocs('Cache miss, processing markdown file');

            // Process markdown content
            const { content, fileInfo } = await processMarkdownContent(DOCS_CONFIG.docPath);

            // Generate table of contents
            const toc = generateTOC(content);

            // Convert markdown to HTML
            const html = await marked.parse(content);

            // Generate complete HTML template
            const fullHTML = generateHTMLTemplate(html, toc, 'Database Utilities Documentation');

            // Cache the result
            cachedData = {
                html: fullHTML,
                lastModified: fileInfo.lastModified,
                size: fileInfo.size,
                tocEntries: toc.length
            };

            await cacheService.set(DOCS_CONFIG.cacheKey, cachedData, DOCS_CONFIG.cacheTTL);

            logger.info('Documentation generated and cached:', {
                size: fileInfo.size,
                tocEntries: toc.length,
                requestId
            });
        } else {
            debugDocs('Cache hit, serving cached documentation');
        }

        // Set appropriate headers
        res.set({
            'Content-Type': 'text/html; charset=utf-8',
            'Cache-Control': 'public, max-age=3600',
            'Last-Modified': new Date(cachedData.lastModified).toUTCString()
        });

        res.send(cachedData.html);

        logger.info('Documentation served successfully:', {
            cached: !!cachedData,
            tocEntries: cachedData.tocEntries,
            requestId
        });

    } catch (error) {
        logger.error('Documentation serving failed:', {
            error: error.message,
            stack: error.stack,
            requestId
        });

        // Send user-friendly error page
        res.status(500).send(`
            <!DOCTYPE html>
            <html>
            <head>
                <title>Documentation Error</title>
                <style>
                    body { font-family: Arial, sans-serif; max-width: 600px; margin: 2rem auto; padding: 2rem; }
                    .error { background: #fee; border: 1px solid #fcc; padding: 1rem; border-radius: 4px; }
                </style>
            </head>
            <body>
                <div class="error">
                    <h2>Documentation Unavailable</h2>
                    <p>Sorry, the documentation is currently unavailable. Please try again later.</p>
                    <p><small>Error ID: ${requestId}</small></p>
                </div>
            </body>
            </html>
        `);
    }
}));

/**
 * Serve raw markdown content
 */
router.get('/raw', asyncHandler(async (req, res) => {
    const requestId = getRequestId();

    try {
        const { content, fileInfo } = await processMarkdownContent(DOCS_CONFIG.docPath);

        res.set({
            'Content-Type': 'text/markdown; charset=utf-8',
            'Content-Disposition': 'inline; filename="database-docs.md"',
            'Cache-Control': 'public, max-age=3600',
            'Last-Modified': new Date(fileInfo.lastModified).toUTCString()
        });

        res.send(content);

        logger.info('Raw markdown served successfully:', {
            size: fileInfo.size,
            requestId
        });

    } catch (error) {
        logger.error('Raw markdown serving failed:', {
            error: error.message,
            requestId
        });

        res.status(500).json({
            error: 'Documentation unavailable',
            message: 'Unable to serve raw markdown content',
            requestId
        });
    }
}));

/**
 * Clear documentation cache (admin endpoint)
 */
router.delete('/cache', asyncHandler(async (req, res) => {
    const requestId = getRequestId();

    try {
        await cacheService.delete(DOCS_CONFIG.cacheKey);

        logger.info('Documentation cache cleared:', { requestId });

        res.json({
            success: true,
            message: 'Documentation cache cleared successfully',
            requestId
        });

    } catch (error) {
        logger.error('Cache clearing failed:', {
            error: error.message,
            requestId
        });

        res.status(500).json({
            error: 'Cache clearing failed',
            message: error.message,
            requestId
        });
    }
}));

/**
 * Documentation metadata endpoint
 */
router.get('/info', asyncHandler(async (req, res) => {
    const requestId = getRequestId();

    try {
        const fileInfo = await validateMarkdownFile(DOCS_CONFIG.docPath);
        const cachedData = await cacheService.get(DOCS_CONFIG.cacheKey);

        res.json({
            file: {
                size: fileInfo.size,
                lastModified: fileInfo.lastModified,
                path: DOCS_CONFIG.docPath
            },
            cache: {
                exists: !!cachedData,
                ttl: DOCS_CONFIG.cacheTTL,
                key: DOCS_CONFIG.cacheKey
            },
            requestId
        });

    } catch (error) {
        logger.error('Documentation info failed:', {
            error: error.message,
            requestId
        });

        res.status(500).json({
            error: 'Unable to get documentation info',
            message: error.message,
            requestId
        });
    }
}));

export default router;