# Use Node.js with alpine for a lightweight environment
FROM node:22.23.4-alpine3.23 AS base

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

# Start the Node.js application in prod mode
CMD ["npm", "start"]
