FROM node:10.17-alpine

# create directory for container
WORKDIR /app

COPY package.json .
COPY package-lock.json .

# installing production packages
RUN npm ci --only=production

# copying the other source code to directory
ADD . .

# typescript compilation
RUN npm run tsc

# starting node application
CMD npm start
