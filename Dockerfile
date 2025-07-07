# Use Node.js with alpine for a lightweight environment
FROM node:lts-alpine3.20 AS base
# Install system dependencies required for the application
RUN apk update && apk add --no-cache \
    python3 \
    make \
    g++ \
    ca-certificates
# Set working directory
WORKDIR /app
# Copy `package.json` and `package-lock.json` to the container
COPY package*.json ./
# Copy the rest of the application source code
COPY . .
# Expose API port
EXPOSE 4000

FROM base AS prod
# Install only production dependencies
RUN npm ci --omit=dev
# Build the TypeScript application
RUN npm run build
# Start the Node.js application in prod mode
CMD ["npm", "start"]

FROM base AS dev
# Install dependencies (including dev)
RUN npm ci
# Start the Node.js application in dev mode
CMD ["npm", "dev"]