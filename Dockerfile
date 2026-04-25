FROM node:20-alpine

WORKDIR /app

# Install deps (cached layer)
COPY package.json ./
RUN npm install --production

# Copy server code
COPY server.js ./

# Default port (override with -e PORT=...)
ENV PORT=8080
EXPOSE 8080

# Healthcheck so orchestrators know if it's alive
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD wget -q -O - http://localhost:${PORT}/health || exit 1

CMD ["node", "server.js"]
