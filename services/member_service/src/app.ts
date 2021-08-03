import { ErrorReporting } from '@google-cloud/error-reporting';
import bodyParser from 'body-parser';
import express, { NextFunction, Request, Response } from 'express';
import { appendMemberDemographics } from './controller/append-member-demographics';
import { createInsurance } from './controller/create-insurance';
import { createMember } from './controller/create-member';
import { createAndPublishMember } from './controller/create-member-and-publish';
import { createZendeskMember } from './controller/create-zendesk-member';
import { deleteInsurance } from './controller/delete-insurance';
import { deleteMember, deleteMemberMrn } from './controller/delete-member';
import { deleteMemberAttribute } from './controller/delete-member-attribute';
import { deleteAddress, deleteEmail, deletePhone } from './controller/delete-member-demographics';
import { getEligibilities } from './controller/get-eligibilities';
import { getInsurance } from './controller/get-insurance';
import { getMember, getMemberByZendeskId } from './controller/get-member';
import { getMemberDemographics } from './controller/get-member-demographics';
import { getMemberInsuranceRecords } from './controller/get-member-insurance-records';
import { getMemberMrnRecords } from './controller/get-member-mrn-records';
import { getMembers } from './controller/get-members';
import { updateAndPublishMember } from './controller/publish-member';
import { resyncMember } from './controller/resync-member';
import { resyncMemberAddresses } from './controller/resync-member-addresses';
import { updateCategory } from './controller/update-category';
import { updateCohort } from './controller/update-cohort';
import { updateEligibilities } from './controller/update-eligibilities';
import { updateInsurance } from './controller/update-insurance';
import { updateMember } from './controller/update-member';
import { updateMemberDemographics } from './controller/update-member-demographics';
import { updateMrn } from './controller/update-mrn';
import { wrapExpressAsync, IExpressError } from './util/error-handlers';

const developmentEnv = 'development';
const API_KEY = process.env.API_KEY;

const errors = new ErrorReporting({
  reportMode: 'production',
  logLevel: 1,
});

const app = express();

// app engine specific 'keep warm' endpoint
app.get('/keep_warm', async (_, res: Response) => res.status(200).send('app active!'));

// authorization via api key in headers
if (!!API_KEY) {
  app.use((req: Request, res: Response, next: NextFunction) => {
    if (!req.get('apiKey')) {
      return res.status(400).json({ error: 'Missing `apiKey` in header' });
    } else if (req.get('apiKey') !== API_KEY) {
      return res.status(403).json({ error: 'apiKey is incorrect' });
    } else {
      next();
    }
  });
} else if (process.env.NODE_ENV !== developmentEnv) {
  throw new Error(`Application missing API_KEY env variable.`);
}
// bodyparser
app.use(bodyParser.json());

// API version
export const API_VERSION = '1.0';

// Configuration of all member endpoints

// Members
app.get(`/${API_VERSION}/members`, getMembers);
app.post(`/${API_VERSION}/members`, createMember);
app.get(`/${API_VERSION}/members/:memberId`, getMember);
app.patch(`/${API_VERSION}/members/:memberId`, updateMember);
app.delete(`/${API_VERSION}/members/:memberId`, deleteMember);
app.post(`/${API_VERSION}/members/createAndPublish`, createAndPublishMember);
app.post(`/${API_VERSION}/members/:memberId/updateAndPublish`, updateAndPublishMember);
app.get(`/${API_VERSION}/members/resync/:memberId`, resyncMember);

// Category & Cohort
app.post(`/${API_VERSION}/members/:memberId/category`, updateCategory);
app.post(`/${API_VERSION}/members/:memberId/cohort`, updateCohort);

// MRN
app.get(`/${API_VERSION}/members/mrn/records`, getMemberMrnRecords);
app.post(`/${API_VERSION}/members/:memberId/mrn`, updateMrn);
app.delete(`/${API_VERSION}/members/:memberId/mrn`, deleteMemberMrn);

// Insurance
app.get(`/${API_VERSION}/members/insurance/records`, getMemberInsuranceRecords);
app.get(`/${API_VERSION}/members/:memberId/insurance`, getInsurance);
app.post(`/${API_VERSION}/members/:memberId/insurance`, createInsurance);
app.patch(`/${API_VERSION}/members/:memberId/insurance`, updateInsurance);
app.delete(`/${API_VERSION}/members/:memberId/insurance/:insuranceId`, deleteInsurance);

// Attributes
app.delete(`/${API_VERSION}/members/:memberId/attributes`, deleteMemberAttribute);

// Demographics
app.get(`/${API_VERSION}/members/:memberId/demographics`, getMemberDemographics);
app.get(`/${API_VERSION}/members/resync/:memberId/demographics/addresses`, resyncMemberAddresses);
app.patch(`/${API_VERSION}/members/:memberId/demographics`, updateMemberDemographics);
app.post(`/${API_VERSION}/members/:memberId/demographics`, appendMemberDemographics);
app.delete(`/${API_VERSION}/members/:memberId/demographics/addresses/:id`, deleteAddress);
app.delete(`/${API_VERSION}/members/:memberId/demographics/emails/:id`, deleteEmail);
app.delete(`/${API_VERSION}/members/:memberId/demographics/phones/:id`, deletePhone);

// Zendesk
app.get(`/${API_VERSION}/members/zendesk/:zendeskId`, getMemberByZendeskId);
app.post(`/${API_VERSION}/members/:memberId/zendesk`, createZendeskMember);

// Eligibilities
app.get(`/${API_VERSION}/members/:memberId/eligibilities`, wrapExpressAsync(getEligibilities));
app.patch(`/${API_VERSION}/members/:memberId/eligibilities`, wrapExpressAsync(updateEligibilities));

// handles errors
// TODO: need explicit tests on the error middleware (to ensure response codes/messages are sent)
app.use(errors.express);
app.use((err: IExpressError, req: Request, res: Response, next: NextFunction) => {
  console.error('Found error on route', err);
  res.status(err.status || 500).send(err.message);
});

export default app;
