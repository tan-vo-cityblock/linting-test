FROM node:14.13.0-alpine

# create directory for container
WORKDIR /app

COPY package.json .
COPY package-lock.json .

# installing production packages
RUN npm ci --only=production

# copying the other source code to directory
COPY . /app/

# typescript compilation
RUN npm run tsc
