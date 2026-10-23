# Multi-stage build for React/Vite application

# Build stage
FROM node:18-alpine AS builder

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --silent

# Copy source code
COPY . .

# Stamp dist/version.json with the building commit (no .git in the
# context). Below `npm ci` on purpose so a new sha doesn't reinstall deps.
ARG RED_EYE_GIT_SHA=""
ENV RED_EYE_GIT_SHA=${RED_EYE_GIT_SHA}

# Build the application
RUN npm run build

# Production stage
FROM nginx:alpine AS production

# Copy built application from builder stage
COPY --from=builder /app/dist /usr/share/nginx/html

# Copy nginx configuration
COPY nginx.conf /etc/nginx/nginx.conf

# Expose port 80
EXPOSE 80

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:80/ || exit 1

# Start nginx
CMD ["nginx", "-g", "daemon off;"]
