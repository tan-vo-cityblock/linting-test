import app from './app';

const PORT = Number(process.env.PORT) || 8080;

// tslint:disable-next-line: no-console
app.listen(PORT, () => console.log(`App listening on port ${PORT}`));
