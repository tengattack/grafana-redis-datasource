FROM node:lts-alpine

ENV NODE_ENV production

COPY package*.json /app/grafana-redis-datasource/

WORKDIR /app/grafana-redis-datasource/

RUN npm install --production

# Bundle app source
COPY . .

WORKDIR /app/grafana-redis-datasource/server/

EXPOSE 3334
USER nobody

CMD ["node", "./redis-proxy.js"]
