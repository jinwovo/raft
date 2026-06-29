# syntax=docker/dockerfile:1
#
# Visualizer image: the Next.js front end on :3010. The browser talks to the backend directly at
# http://localhost:8104 (NEXT_PUBLIC_RAFT_BASE), so only the two host ports need publishing — no
# container-to-container networking is required for the demo.

FROM node:22-alpine AS build
WORKDIR /web
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM node:22-alpine
WORKDIR /web
ENV NODE_ENV=production
COPY --from=build /web ./
EXPOSE 3010
CMD ["npm", "run", "start"]
