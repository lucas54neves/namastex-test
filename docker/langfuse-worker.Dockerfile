FROM docker.io/langfuse/langfuse-worker:3

RUN set -eu; \
    redis_file="$(find /app/worker/node_modules/.pnpm -path '*/@langfuse/shared/dist/src/server/redis/redis.js' -print -quit)"; \
    test -n "$redis_file"; \
    sed -i "s/socketTimeout: 30000/socketTimeout: undefined/" "$redis_file"; \
    grep -q "socketTimeout: undefined" "$redis_file"
