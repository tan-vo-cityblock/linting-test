import { Request } from 'express';
import { isEmpty } from 'lodash';
import validator from 'validator';
import { ICreateMemberRequest } from '../controller/types';

export function demographicsValidation(request: Request): string[] {
  const { demographics }: ICreateMemberRequest = request.body;
  const errorBody: string[] = [];

  let firstName;
  let lastName;
  let dob;
  let sex;

  // check to see if legacy fields are being used to update the Member's primary info and if so, map the fields for checking accordingly
  if (isEmpty(demographics)) {
    firstName = request.body.firstName;
    lastName = request.body.lastName;
    dob = request.body.dob;
    sex = request.body.sex;
  } else {
    firstName = demographics.firstName;
    lastName = demographics.lastName;
    dob = demographics.dateOfBirth;
    sex = demographics.sex;
  }

  if (!firstName || !(typeof firstName === 'string')) {
    errorBody.push('First Name must be a string');
  }
  if (!lastName || !(typeof lastName === 'string')) {
    errorBody.push('Last Name must be a string');
  }
  if (!dob || !validator.isISO8601(dob)) {
    errorBody.push('DOB must be of the format YYYY-MM-DD');
  }
  // TODO: make sex have a collapsible list of entries.
  if (sex && !validator.isAlpha(sex)) {
    errorBody.push('Sex must be alphabetic');
  }

  return errorBody;
}