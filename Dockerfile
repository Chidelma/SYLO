FROM oven/bun:1.2.19

WORKDIR /app

ENV NODE_ENV=production

COPY package.json bun.lockb ./

RUN bun install --frozen-lockfile

COPY --chown=bun:bun . .

USER bun
