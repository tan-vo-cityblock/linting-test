// NOTE: This is a default / dummy service to enable deployment of multiple services in the same GCP App Engine.
// See for more details: https://cloud.google.com/appengine/docs/flexible/nodejs/configuration-files#the_default_service

const express = require('express');

const app = express();

app.get('/', (req, res) => {
  res
    .status(200)
    .send('Hello, world! Welcome to CBH Services.')
    .end();
});

// Start the server
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`App listening on port ${PORT}`);
  console.log('Press Ctrl+C to quit.');
});

module.exports = app;
