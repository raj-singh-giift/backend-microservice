# Production-Ready Node.js Backend Project Structure

## Project Structure
```
production-backend/
├── src/
│   ├── app.js                    # Main application entry point
│   ├── server.js                 # HTTP/HTTPS server setup
│   ├── config/
│   │   ├── index.js              # Configuration management
│   │   ├── database.js           # Database configuration
│   │   ├── redis.js              # Redis configuration
│   │   └── logger.js             # Winston logger configuration
│   ├── middleware/
│   │   ├── auth.js               # JWT authentication middleware
│   │   ├── validation.js         # Joi validation middleware
│   │   ├── errorHandler.js       # Global error handling
│   │   ├── security.js           # Security middleware
│   │   ├── rateLimiter.js        # Rate limiting
│   │   └── requestTracker.js     # Request tracking with cls-rtracker
│   ├── routes/
│   │   ├── index.js              # Route aggregator
│   │   ├── auth.js               # Authentication routes
│   │   ├── users.js              # User management routes
│   │   └── health.js             # Health check routes
│   ├── controllers/
│   │   ├── authController.js     # Authentication logic
│   │   ├── userController.js     # User management logic
│   │   └── healthController.js   # Health check logic
│   ├── models/
│   │   ├── index.js              # Database models aggregator
│   │   └── User.js               # User model
│   ├── services/
│   │   ├── authService.js        # Authentication service
│   │   ├── userService.js        # User service
│   │   ├── cacheService.js       # Redis cache service
│   │   ├── cronService.js        # Cron job service
│   │   └── httpService.js        # Reusable HTTP client service
│   ├── utils/
│   │   ├── database.js           # Database utility functions
│   │   ├── crypto.js             # Crypto utilities
│   │   ├── sanitizer.js          # Data sanitization
│   │   └── validator.js          # Custom validators
│   ├── schemas/
│   │   ├── authSchemas.js        # Joi schemas for authentication
│   │   └── userSchemas.js        # Joi schemas for user operations
│   └── constants/
│       ├── statusCodes.js        # HTTP status codes
│       └── errorMessages.js      # Error message constants
├── logs/                         # Log files directory
├── uploads/                      # File uploads directory
├── views/                        # EJS templates
├── public/                       # Static files
├── .env                          # Environment variables
├── .env.example                  # Environment variables example
├── .gitignore                    # Git ignore file
├── .eslintrc.js                  # ESLint configuration
├── nodemon.json                  # Nodemon configuration
├── docker-compose.yml            # Docker compose for dev environment
├── Dockerfile                    # Docker configuration
└── README.md                     # Project documentation
```

## Environment Variables (.env.example)
```env
# Application
NODE_ENV=development
PORT=3000
APP_NAME=Production Backend API
APP_VERSION=1.0.0

# SSL/TLS
USE_HTTPS=false
SSL_CERT_PATH=./certs/cert.pem
SSL_KEY_PATH=./certs/key.pem

# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=production_db
DB_USER=postgres
DB_PASSWORD=your_password
DB_SSL=false
DB_POOL_MIN=2
DB_POOL_MAX=10

# Redis
REDIS_ENABLED=true  # Set to 'false' to disable Redis completely
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
REDIS_DB=0
REDIS_TTL=3600

# JWT
JWT_SECRET=your_super_secret_jwt_key_here
JWT_REFRESH_SECRET=your_refresh_secret_here
JWT_EXPIRE_TIME=1h
JWT_REFRESH_EXPIRE_TIME=7d

# Logging
LOG_LEVEL=info
LOG_FILE_ENABLED=true
LOG_MAX_SIZE=20m
LOG_MAX_FILES=14d

# Rate Limiting
RATE_LIMIT_WINDOW_MS=900000
RATE_LIMIT_MAX_REQUESTS=100

# CORS
CORS_ORIGINS=http://localhost:3000,http://localhost:3001
CORS_CREDENTIALS=true

# Security
BCRYPT_SALT_ROUNDS=12
CSRF_SECRET=your_csrf_secret
```

## Redis Configuration

The application supports conditional Redis usage through the `REDIS_ENABLED` environment variable:

### Enable Redis (Default)
```env
REDIS_ENABLED=true
```

### Disable Redis
```env
REDIS_ENABLED=false
```

When Redis is disabled:
- No Redis connection will be attempted
- All cache operations will be skipped gracefully
- The application will continue to function normally
- Cache-related functions will return appropriate default values
- Health checks will show Redis as "disabled"

This feature is useful for:
- Development environments without Redis
- Testing scenarios
- Production deployments where Redis is not available
- Cost optimization in certain deployment scenarios
```

## Docker Configuration

### Dockerfile
```dockerfile
FROM node:18-alpine

WORKDIR /app

# Install dumb-init for proper signal handling
RUN apk add --no-cache dumb-init

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production && npm cache clean --force

# Create non-root user
RUN addgroup -g 1001 -S nodejs
RUN adduser -S nodejs -u 1001

# Copy application code
COPY --chown=nodejs:nodejs . .

# Create directories with proper permissions
RUN mkdir -p logs uploads && chown -R nodejs:nodejs logs uploads

USER nodejs

EXPOSE 3000

ENTRYPOINT ["dumb-init", "--"]
CMD ["npm", "start"]
```

### docker-compose.yml
```yaml
version: '3.8'

services:
  app:
    build: .
    ports:
      - "3000:3000"
    environment:
      - NODE_ENV=production
    depends_on:
      - postgres
      - redis
    volumes:
      - ./logs:/app/logs
    restart: unless-stopped

  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_DB: production_db
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: your_password
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    restart: unless-stopped

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    restart: unless-stopped

volumes:
  postgres_data:
  redis_data:
```

## Quick Setup Instructions

1. **Clone and Install:**
   ```bash
   git clone <your-repo>
   cd production-backend
   npm install
   ```

2. **Environment Setup:**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Database Setup:**
   ```bash
   # Make sure PostgreSQL and Redis are running
   npm run dev
   ```

4. **Production Deployment:**
   ```bash
   docker-compose up -d
   ```

## Key Features Implemented

✅ **Winston Logging** - Structured logging with file rotation  
✅ **Joi Validation** - Schema validation for all endpoints  
✅ **Parameterized Queries** - SQL injection protection  
✅ **Reusable Functions** - Database, HTTP, and utility functions  
✅ **HTTPS/HTTP Setup** - Environment-based SSL configuration  
✅ **PostgreSQL** - Production-ready database setup  
✅ **Redis Caching** - Session and data caching  
✅ **JWT Authentication** - Secure token-based auth  
✅ **Compression** - Gzip compression middleware  
✅ **Security Headers** - Helmet.js security  
✅ **CORS** - Multi-origin support  
✅ **Rate Limiting** - DDoS protection  
✅ **Request Tracking** - Async context tracking  
✅ **Data Sanitization** - Input sanitization  
✅ **Cron Jobs** - Scheduled task management  
✅ **Error Handling** - Centralized error management  
✅ **Docker Support** - Containerized deployment


Production Backend Setup Instructions

## 1. Project Initialization

\`\`\`bash
# Create project directory
mkdir production-backend
cd production-backend

# Initialize npm project
npm init -y

# Install dependencies (copy package.json content)
npm install

# Create directory structure
mkdir -p src/{config,middleware,routes,controllers,models,services,utils,schemas,constants}
mkdir -p logs uploads views public

# Copy all source files to their respective directories
\`\`\`

## 2. Environment Setup

\`\`\`bash
# Create environment file
cp .env.example .env

# Edit .env with your configuration
nano .env
\`\`\`

## 3. Database Setup

\`\`\`bash
# Connect to PostgreSQL
psql -U postgres

# Create database
CREATE DATABASE production_db;

# Connect to database
\\c production_db;

# Run schema (copy from databaseSchema above)
\\i schema.sql
\`\`\`

## 4. SSL Certificates (for HTTPS)

\`\`\`bash
# Create certs directory
mkdir certs

# Generate self-signed certificate for development
openssl req -x509 -newkey rsa:4096 -keyout certs/key.pem -out certs/cert.pem -days 365 -nodes

# For production, use Let's Encrypt or your certificate provider
\`\`\`

## 5. Development

\`\`\`bash
# Start development server
npm run dev

# Start with debugging
npm run debug

# Run tests
npm test

# Check for security vulnerabilities
npm run security-audit
\`\`\`

## 6. Production Deployment

\`\`\`bash
# Using Docker
docker-compose up -d

# Or using PM2
npm install -g pm2
pm2 start ecosystem.config.js
\`\`\`

## 7. Monitoring and Logs

- Logs are stored in \`logs/\` directory
- Health check available at \`/health\`
- Detailed health check at \`/health/detailed\`
- Application metrics available through endpoints

## 8. Security Checklist

- [ ] Change default JWT secrets
- [ ] Set up proper CORS origins
- [ ] Configure rate limiting
- [ ] Set up SSL certificates
- [ ] Enable security headers
- [ ] Set up monitoring and alerting
- [ ] Configure backup strategy
- [ ] Set up log rotation
- [ ] Implement proper error handling
- [ ] Set up CI/CD pipeline

## 9. API Documentation

The API provides the following main endpoints:

### Authentication
- \`POST /api/auth/register\` - User registration
- \`POST /api/auth/login\` - User login
- \`POST /api/auth/logout\` - User logout
- \`POST /api/auth/refresh\` - Refresh access token
- \`POST /api/auth/forgot-password\` - Request password reset
- \`POST /api/auth/reset-password\` - Reset password
- \`POST /api/auth/change-password\` - Change password
- \`GET /api/auth/verify-email/:token\` - Verify email
- \`GET /api/auth/me\` - Get current user

### User Management
- \`GET /api/users/profile\` - Get user profile
- \`PUT /api/users/profile\` - Update user profile
- \`DELETE /api/users/profile\` - Delete user account
- \`GET /api/users\` - Get all users (admin)
- \`GET /api/users/:id\` - Get user by ID (admin)
- \`PUT /api/users/:id\` - Update user (admin)
- \`DELETE /api/users/:id\` - Delete user (admin)

### Health Checks
- \`GET /health\` - Basic health check
- \`GET /health/detailed\` - Detailed health check
- \`GET /health/ready\` - Readiness probe
- \`GET /health/live\` - Liveness probe

## 10. Features Implemented

✅ **Production Ready Features:**
- Winston logging with file rotation
- Joi schema validation
- Parameterized SQL queries
- JWT authentication
- Redis caching
- Rate limiting
- Security headers (Helmet)
- CORS configuration
- Request compression
- Error handling
- Health checks
- Cron jobs
- Request tracking
- Data sanitization
- Password hashing
- File upload support
- Environment-based configuration
- Docker support
- Database connection pooling
- Graceful shutdown
- Memory usage monitoring

This setup provides a robust, secure, and scalable foundation for a production Node.js backend application.