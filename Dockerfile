# Use Node.js with alpine for a lightweight environment
FROM node:lts-alpine3.20 AS base

# Set working directory
WORKDIR /app
# Copy `package.json` and `package-lock.json` to the container
COPY package*.json ./
RUN npm ci --omit=dev

# Copy the rest of the application source code
COPY . .
# Expose API port
EXPOSE 4000
ENV PORT=4000

# Install only production dependencies
# Build the TypeScript application
RUN npm run build
# Start the Node.js application in prod mode
CMD ["npm", "start"]
