import { AxiosError } from 'axios';
import { NextFunction, Request, Response } from 'express';
export interface IExpressError {
  status: number;
  message: string;
}

export const wrapExpressAsync = (fn) => (req: Request, res: Response, next: NextFunction) =>
  Promise.resolve(fn(req, res, next)).catch(next);

// tslint:disable no-console
export async function handleAxiosError(service: string, error: AxiosError) {
  const headerTag = `[ ${service.toUpperCase()} ERROR ]`;
  console.error(
    `${headerTag} got error in ${service} method: [${error.config.method}], endpoint: [${error.config.url}]...`,
  );
  if (error.response) {
    const { status, statusText } = error.response;
    console.log(`${headerTag} Have response [status: ${status}, statusText: ${statusText}]
    with data: ${JSON.stringify(error.response.data)}`);
  } else if (error.request) {
    console.log(
      `${headerTag} only have the request object available [request:`,
      error.request,
      ']',
    );
  } else {
    console.log(`${headerTag} no response or request available!`, error.message);
  }
  return Promise.reject(error);
}
// tslint:enable no-console
