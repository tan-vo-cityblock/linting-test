# same Node version used by Commons
FROM node:14.15.1

# create directory for container
WORKDIR /app

COPY package.json .
COPY package-lock.json .

# installing packages
RUN npm ci

# copying the other source code to directory
ADD . .

# typescript compilation
RUN npm run build