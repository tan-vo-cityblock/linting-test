import { Transaction } from 'objection';
import { IAddress } from '../models/address';
import { IEmail } from '../models/email';
import { IInsurancePlan, IInsurance } from '../models/member-insurance';
import { IMemberDemographics } from '../models/member-demographics';
import { IPhone } from '../models/phone';

// TODO: potentially extend the ICreateAndPublish interface with the additional patient fields.
export interface IMemberPubSubMessage {
  patientId: string;
  cityblockId?: number;

  // required fields for first time creation
  clinicId?: string;
  memberId?: string;
  nmi?: string;
  marketId?: string;
  medicalCenterName?: string;
  partnerId?: string;
  pcpAddress?: string;
  pcpName?: string;
  pcpPhone?: string;
  pcpPractice?: string;

  // demographic fields
  firstName?: string;
  middleName?: string;
  lastName?: string;
  gender?: string;
  medicareId?: string;
  shouldUpdateAddress?: boolean;
  addressLine1?: string;
  addressLine2?: string;
  city?: string;
  county?: string;
  state?: string;
  zip?: string;
  ssn?: string;
  dob?: string;
  dod?: string;
  email?: string;
  phone?: string;
  race?: string;
  ethnicity?: string;
  language?: string;
  maritalStatus?: string;

  // insurance fields
  acuityRank?: number;
  acpnyInNetwork?: boolean;
  cohortName?: string;
  insurance?: string;
  lineOfBusiness?: string;
  medicaidId?: string;
  medicaidPremiumGroup?: string;
  mrn?: string;
  planDescription?: string;
  productDescription?: string;
}

export interface ICreateMemberInternalArgs {
  createMemberRequest: ICreateMemberRequest;
  clientSource?: string;
  elationResponseId?: string;
  testTransaction?: Transaction;
}

export interface ICreateAndPublishMemberRequest extends ICreateMemberRequest {
  // information needed for publishing data to Commons
  pcpAddress?: string;
  pcpName?: string;
  pcpPhone?: string;
  pcpPractice?: string;
  marketId?: string;
  partnerId?: string;
  clinicId?: string;
}

export interface ICreateMemberRequest {
  partner?: string;
  insurances?: IInsurance[];
  primaryPhysician?: number;
  caregiverPractice?: number;
  elationPlan?: string;
  createElationPatient?: boolean;
  // TODO resolve how these should be passed in? explicit ID, or name then retrieve ID from database?
  cohortId?: number;
  categoryId?: number;
  tags?: string[];
  demographics?: IMemberDemographics;
  medicaid?: IInsurance[];
  medicare?: IInsurance[];
}

export interface IUpdateMemberDemographicsRequest {
  dateOfBirth?: string;
  dateOfDemise?: string;
  isMarkedDeceased?: boolean;
  ethnicity?: string;
  race?: string;
  language?: string;
  firstName?: string;
  gender?: string;
  lastName?: string;
  maritalStatus?: string;
  middleName?: string;
  sex?: string;
  updatedBy: string;
}

export interface IAppendMemberDemographicsRequest {
  addresses?: IAddress[];
  phones?: IPhone[];
  emails?: IEmail[];
  updatedBy: string;
}

export interface IUpdateInsuranceRequest {
  carrier: string;
  plans: IInsurancePlan[];
  updatedBy?: string;
}

export interface IUpdateMemberRequest {
  demographics?: IMemberDemographics;
  insurances?: IInsurance[];
  zendeskId?: string;
  tags?: string[];
  elationPlan?: string;
  updatedBy?: string;
}
