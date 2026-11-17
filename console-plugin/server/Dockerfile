FROM registry.access.redhat.com/ubi9/nodejs-20

USER root
RUN mkdir -p /app && chown -R 1001:0 /app && chmod -R g+rwX /app
WORKDIR /app

COPY --chown=1001:0 package.json ./
USER 1001
RUN npm install --omit=dev --no-audit --no-fund

USER root
COPY --chown=1001:0 . .
USER 1001

ENV PORT=4001
ENV NODE_ENV=production

EXPOSE 4001

CMD ["node", "server.js"]
