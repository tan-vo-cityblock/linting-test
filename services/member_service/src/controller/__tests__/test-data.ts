import { IInsurance } from '../../models/member-insurance';
import { IMemberDemographics } from '../../models/member-demographics';
import {
  IAppendMemberDemographicsRequest,
  ICreateAndPublishMemberRequest,
  ICreateMemberRequest,
  IMemberPubSubMessage,
  IUpdateMemberDemographicsRequest,
  IUpdateMemberRequest,
} from '../types';

export const createAndPublishMemberRequest: ICreateAndPublishMemberRequest = {
  demographics: {
    firstName: 'Perry',
    lastName: 'Platypus',
    dateOfBirth: '1980-01-01',
    isMarkedDeceased: false,
    sex: 'male',
    race: 'Martian',
    language: 'platypan',
    addresses: [
      {
        street1: '1 Main Street',
        street2: 'Apartment 1',
        city: 'Winterfell',
        state: 'The North',
        zip: '11111',
      },
    ],
    phones: [
      {
        phone: '213-234-1635',
      },
    ],
    emails: [
      {
        email: 'terry@gmail.com',
      },
    ],
  },
  partner: 'emblem',
  insurances: [
    {
      carrier: 'emblem',
      plans: [
        {
          externalId: '8435092485904285',
          current: true,
          rank: 'primary',
          details: [
            {
              lineOfBusiness: 'medicare',
              subLineOfBusiness: 'dual',
            },
          ],
        },
      ],
    },
  ],
  marketId: '13633ece-0356-493d-b3f5-5ee12fdb1892',
  partnerId: 'e7db7bd3-327d-4139-b3cb-f376c6a32462',
  clinicId: 'cf0da529-23c6-401c-8a2c-ab3add6beb9d',
  pcpAddress: '2832 Linden Blvd Brooklyn, NY 112085132',
  pcpName: 'Mary Yia',
  pcpPhone: '7182402000',
  pcpPractice: 'Lindenwood Center ACP',
  cohortId: 1,
  medicaid: [
    {
      carrier: 'medicaidNY',
      plans: [{ externalId: '12' }],
    },
  ],
  medicare: [
    {
      carrier: 'medicareNY',
      plans: [{ externalId: '21' }],
    },
  ],
};

// TODO: extend the previous object if this one is a strict subset
export function memberPubSubMessage(
  patientId: string,
  cityblockId: number,
  mrn: string,
): IMemberPubSubMessage {
  return {
    patientId,
    cityblockId,
    memberId: '8435092485904285',
    nmi: '8435092485904285',
    mrn,
    marketId: '13633ece-0356-493d-b3f5-5ee12fdb1892',
    partnerId: 'e7db7bd3-327d-4139-b3cb-f376c6a32462',
    clinicId: 'cf0da529-23c6-401c-8a2c-ab3add6beb9d',
    pcpAddress: '2832 Linden Blvd Brooklyn, NY 112085132',
    pcpName: 'Mary Yia',
    pcpPhone: '7182402000',
    pcpPractice: 'Lindenwood Center ACP',
    firstName: 'Perry',
    lastName: 'Platypus',
    gender: 'male',
    addressLine1: '1 Main Street',
    addressLine2: 'Apartment 1',
    city: 'Winterfell',
    state: 'The North',
    zip: '11111',
    dob: '1980-01-01',
    email: 'terry@gmail.com',
    phone: '213-234-1635',
    race: 'Martian',
    language: 'platypan',
    cohortName: 'Emblem Cohort 1',
    insurance: 'emblem',
    lineOfBusiness: 'medicare',
    medicareId: '21',
    medicaidId: '12',
    productDescription: 'dual',
  };
}

export const resyncPubSubMessage: IMemberPubSubMessage = {
  patientId: 'a3ccab32-d960-11e9-b350-acde48001122',
  memberId: 'M2',
  nmi: 'M2',
  cohortName: 'Emblem Cohort 1',
  insurance: 'emblem',
  lineOfBusiness: 'medicare',
  productDescription: null,
};

export const resyncAddressesPubSubMessage: IMemberPubSubMessage = {
  patientId: 'a3ccab32-d960-11e9-b350-acde48001122',
  memberId: 'M2',
  nmi: 'M2',
  shouldUpdateAddress: true,
  addressLine1: '25 Rugby Road',
  addressLine2: null,
  city: 'Brooklyn',
  county: null,
  state: 'NY',
  zip: '11223',
  insurance: 'emblem',
  lineOfBusiness: 'medicare',
  productDescription: null,
};

export const createMemberRequest: ICreateMemberRequest = {
  partner: 'emblem',
  primaryPhysician: 1,
  caregiverPractice: 2,
  insurances: [
    {
      carrier: 'emblem',
      plans: [
        {
          rank: 'primary',
          current: true,
          externalId: '8435092485904285',
          details: [
            {
              lineOfBusiness: 'medicare',
              subLineOfBusiness: 'medicare advantage',
            },
          ],
        },
      ],
    },
  ],
  demographics: {
    firstName: 'Per',
    lastName: 'Plat',
    dateOfBirth: '1980-01-01',
    isMarkedDeceased: false,
    sex: 'male',
    emails: [{ email: 'tdawg@gmail.com' }],
    phones: [{ phone: '6159530693' }],
  },
  medicaid: [
    {
      carrier: 'medicaidNY',
      plans: [
        {
          rank: 'secondary',
          current: true,
          externalId: 'medicaid-id',
          details: [
            {
              lineOfBusiness: 'medicaid',
            },
          ],
        },
      ],
    },
  ],
  medicare: [
    {
      carrier: 'medicareNY',
      plans: [
        {
          rank: 'secondary',
          current: true,
          externalId: 'medicare-id',
          details: [
            {
              lineOfBusiness: 'medicare',
            },
          ],
        },
      ],
    },
  ],
};

export const failingCreateRequest: ICreateMemberRequest = {
  demographics: {
    firstName: 'Jerry',
    lastName: 'Platypus',
    dateOfBirth: '19801980',
    isMarkedDeceased: false,
    sex: 'male',
  },
  insurances: [
    {
      carrier: 'tufts',
      plans: [
        {
          externalId: '8435092485904286',
          details: [{ lineOfBusiness: 'medicare' }],
        },
      ],
    },
  ],
  partner: 'tufts',
  primaryPhysician: 1,
  caregiverPractice: 2,
};

export const failingRequestForElation: ICreateMemberRequest = {
  demographics: {
    firstName: 'Terry',
    lastName: 'Platypus',
    dateOfBirth: '1980-01-01',
    isMarkedDeceased: false,
    sex: 'male',
  },
  insurances: [
    {
      carrier: 'tufts',
      plans: [
        {
          externalId: '8435092485904286',
          details: [{ lineOfBusiness: 'medicare' }],
        },
      ],
    },
  ],
  partner: 'tufts',
  primaryPhysician: 1,
  caregiverPractice: 2,
};

export const partialPublishMemberRequest: ICreateAndPublishMemberRequest = {
  demographics: {
    firstName: 'Terry',
    lastName: 'Machine',
    dateOfBirth: '1970-01-01',
    isMarkedDeceased: false,
    maritalStatus: 'currentlyMarried',
    race: 'Martian',
    language: 'platypan',
  },
  marketId: '13633ece-0356-493d-b3f5-5ee12fdb1892',
  partnerId: 'e7db7bd3-327d-4139-b3cb-f376c6a32462',
  clinicId: 'cf0da529-23c6-401c-8a2c-ab3add6beb9d',
};

export function partialMemberPubSubMessage(
  patientId: string,
  cityblockId: number,
  mrn: string,
): IMemberPubSubMessage {
  return {
    patientId,
    cityblockId,
    mrn,
    marketId: '13633ece-0356-493d-b3f5-5ee12fdb1892',
    partnerId: 'e7db7bd3-327d-4139-b3cb-f376c6a32462',
    clinicId: 'cf0da529-23c6-401c-8a2c-ab3add6beb9d',
    firstName: 'Terry',
    lastName: 'Machine',
    dob: '1970-01-01',
    race: 'Martian',
    language: 'platypan',
    maritalStatus: 'currentlyMarried',
  };
}

export const createZendeskMemberRequest = {
  organizationName: 'Emblem Medicaid Virtual',
  primary_hub: 'Emblem Medicaid Virtual',
  care_model: 'Community Integrated Care',
  primary_chp: 'Test CHP',
};

export const createMemberInsuranceRequest: IInsurance = {
  carrier: 'emblem',
  plans: [
    {
      externalId: 'insuranceWithDetails',
      rank: 'primary',
      current: true,
      details: [
        {
          lineOfBusiness: 'commercial',
          subLineOfBusiness: 'fully insured',
          spanDateStart: '2019-10-01',
        },
        {
          lineOfBusiness: 'commercial',
          subLineOfBusiness: 'fully insured',
          spanDateStart: '2018-01-01',
          spanDateEnd: '2018-12-31',
        },
      ],
    },
  ],
};

export const updateMemberInsuranceRequest: IInsurance = {
  carrier: 'emblem',
  plans: [
    {
      rank: 'primary',
      current: true,
      externalId: 'M3',
      details: [],
    },
  ],
};

// Request bodies for UpdateInsurance tests
export const updateSingleDetailsRequest: IInsurance = {
  carrier: 'emblem',
  plans: [
    {
      rank: 'primary',
      current: true,
      externalId: 'M8',
      details: [
        {
          lineOfBusiness: null,
          subLineOfBusiness: null,
          spanDateStart: '2021-02-01',
          spanDateEnd: '2021-03-01',
        },
      ],
    },
  ],
};

export const updateMultipleDetailsRequest: IInsurance = {
  carrier: 'emblem',
  plans: [
    {
      rank: 'primary',
      current: true,
      externalId: 'M9',
      details: [
        {
          lineOfBusiness: null,
          subLineOfBusiness: null,
          spanDateStart: '2020-11-01',
          spanDateEnd: '2021-02-01',
        },
        {
          lineOfBusiness: null,
          subLineOfBusiness: null,
          spanDateStart: '2021-03-01',
          spanDateEnd: '2021-04-01',
        },
      ],
    },
  ],
};

export const updateDetailsGapRemovalRequest: IInsurance = {
  carrier: 'emblem',
  plans: [
    {
      rank: 'primary',
      current: true,
      externalId: 'M10',
      details: [
        {
          lineOfBusiness: null,
          subLineOfBusiness: null,
          spanDateStart: '2020-11-01',
          spanDateEnd: '2021-04-01',
        },
      ],
    },
  ],
};

export const updateDetailsGapAdditionRequest: IInsurance = {
  carrier: 'emblem',
  plans: [
    {
      rank: 'primary',
      current: true,
      externalId: 'M11',
      details: [
        {
          lineOfBusiness: null,
          subLineOfBusiness: null,
          spanDateStart: '2020-11-01',
          spanDateEnd: '2021-02-01',
        },
        {
          lineOfBusiness: null,
          subLineOfBusiness: null,
          spanDateStart: '2021-03-01',
          spanDateEnd: '2021-04-01',
        },
      ],
    },
  ],
};

export const updateDetailsLobSlobChangeRequest: IInsurance = {
  carrier: 'emblem',
  plans: [
    {
      rank: 'primary',
      current: true,
      externalId: 'M12',
      details: [
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2020-11-01',
          spanDateEnd: '2021-04-01',
        },
      ],
    },
  ],
};

export const updateDetailsDateAndLobSlobChangeRequest: IInsurance = {
  carrier: 'emblem',
  plans: [
    {
      rank: 'primary',
      current: true,
      externalId: 'M13',
      details: [
        {
          lineOfBusiness: 'medicare',
          subLineOfBusiness: 'medicare',
          spanDateStart: '2020-11-01',
          spanDateEnd: '2020-12-01',
        },
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2020-12-01',
          spanDateEnd: '2021-03-01',
        },
      ],
    },
  ],
};

export const updateDetailsNoChangeRequest: IInsurance = {
  carrier: 'emblem',
  plans: [
    {
      rank: 'primary',
      current: true,
      externalId: 'M14',
      details: [
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2020-11-01',
          spanDateEnd: '2021-04-01',
        },
      ],
    },
  ],
};

export const updateDetailsForMultipleExternalIdsRequest: IInsurance = {
  carrier: 'emblem',
  plans: [
    {
      rank: 'primary',
      current: true,
      externalId: 'M15',
      details: [
        {
          lineOfBusiness: 'commercial',
          subLineOfBusiness: 'exchange',
          spanDateStart: '2020-11-01',
          spanDateEnd: '2021-04-01',
        },
      ],
    },
    {
      rank: 'primary',
      current: true,
      externalId: 'M16',
      details: [
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2021-05-01',
          spanDateEnd: '2021-06-01',
        },
      ],
    },
  ],
};

export const updateDetailsForOneExternalIdRequest: IInsurance = {
  carrier: 'emblem',
  plans: [
    {
      rank: 'primary',
      current: true,
      externalId: 'M15',
      details: [
        {
          lineOfBusiness: 'commercial',
          subLineOfBusiness: 'exchange',
          spanDateStart: '2020-11-01',
          spanDateEnd: '2021-01-01',
        },
      ],
    },
    {
      rank: 'primary',
      current: true,
      externalId: 'M16',
      details: [
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2021-02-01',
          spanDateEnd: '2021-04-01',
        },
      ],
    },
  ],
};

export const updateDetailsForOneExternalIdGapAdditionRequest: IInsurance = {
  carrier: 'emblem',
  plans: [
    {
      rank: 'primary',
      current: true,
      externalId: 'M15',
      details: [
        {
          lineOfBusiness: 'commercial',
          subLineOfBusiness: 'exchange',
          spanDateStart: '2020-11-01',
          spanDateEnd: '2021-02-01',
        },
      ],
    },
    {
      rank: 'primary',
      current: true,
      externalId: 'M16',
      details: [
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2021-01-01',
          spanDateEnd: '2021-04-01',
        },
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2021-05-01',
          spanDateEnd: '2021-06-01',
        },
      ],
    },
  ],
};

export const updateDetailsForOneExternalIdHistoryChangeRequest: IInsurance = {
  carrier: 'emblem',
  plans: [
    {
      rank: 'primary',
      current: true,
      externalId: 'M17',
      details: [
        {
          lineOfBusiness: 'commercial',
          subLineOfBusiness: 'exchange',
          spanDateStart: '2020-11-01',
          spanDateEnd: '2021-02-01',
        },
      ],
    },
    {
      rank: 'primary',
      current: true,
      externalId: 'M18',
      details: [
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2021-01-01',
          spanDateEnd: '2021-06-01',
        },
      ],
    },
  ],
};

export const updateSingleDetails3YearsOldRequest: IInsurance = {
  carrier: 'emblem',
  plans: [
    {
      rank: 'primary',
      current: true,
      externalId: 'M24',
      details: [
        {
          lineOfBusiness: null,
          subLineOfBusiness: null,
          spanDateStart: '2018-02-01',
          spanDateEnd: '2021-02-01',
        },
      ],
    },
  ],
};

export const updateMultipleDetailsForTwoExternalIdsRequest: IInsurance = {
  carrier: 'emblem',
  plans: [
    {
      rank: 'primary',
      current: true,
      externalId: 'M19',
      details: [
        {
          lineOfBusiness: 'commercial',
          subLineOfBusiness: 'exchange',
          spanDateStart: '2020-10-01',
          spanDateEnd: '2021-05-01',
        },
      ],
    },
    {
      rank: 'primary',
      current: true,
      externalId: 'M20',
      details: [
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2020-08-01',
          spanDateEnd: '2020-11-01',
        },
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2021-01-01',
          spanDateEnd: '2021-04-01',
        },
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2021-05-01',
          spanDateEnd: '2021-06-01',
        },
      ],
    },
  ],
};

export const updateDetailsFromOverlappingSpansRequest: IInsurance = {
  carrier: 'emblem',
  plans: [
    {
      rank: 'primary',
      current: true,
      externalId: 'M21',
      details: [
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2020-11-01',
          spanDateEnd: '2020-12-01',
        },
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2020-12-02',
          spanDateEnd: '2021-01-01',
        },
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2021-01-02',
          spanDateEnd: '2021-02-01',
        },
      ],
    },
  ],
};
export const updateDetailsFromOverlappingSpansForMultipleExternalIdsRequest: IInsurance = {
  carrier: 'emblem',
  plans: [
    {
      rank: 'primary',
      current: true,
      externalId: 'M22',
      details: [
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2020-11-01',
          spanDateEnd: '2020-12-01',
        },
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2020-12-02',
          spanDateEnd: '2021-01-01',
        },
        {
          lineOfBusiness: 'medicaid',
          subLineOfBusiness: 'medicaid',
          spanDateStart: '2021-01-02',
          spanDateEnd: '2021-02-01',
        },
      ],
    },
    {
      rank: 'primary',
      current: true,
      externalId: 'M23',
      details: [
        {
          lineOfBusiness: 'commercial',
          subLineOfBusiness: 'exchange',
          spanDateStart: '2020-10-01',
          spanDateEnd: '2020-11-01',
        },
        {
          lineOfBusiness: 'commercial',
          subLineOfBusiness: 'exchange',
          spanDateStart: '2020-11-02',
          spanDateEnd: '2020-12-01',
        },
        {
          lineOfBusiness: 'commercial',
          subLineOfBusiness: 'exchange',
          spanDateStart: '2020-12-02',
          spanDateEnd: '2021-01-01',
        },
      ],
    },
  ],
};

export const updateMemberDemographicsRequest: IUpdateMemberDemographicsRequest = {
  firstName: 'Jerry',
  lastName: 'Platypus-Platt',
  dateOfBirth: '1975-12-12',
  maritalStatus: 'divorced',
  isMarkedDeceased: true,
  updatedBy: '5b36fcd0-b5c6-4b98-a343-465107765420',
};

export const appendEmailsDemographicsRequest: IAppendMemberDemographicsRequest = {
  emails: [{ email: 'tdawg@gmail.com' }],
  updatedBy: '5b36fcd0-b5c6-4b98-a343-465107765420',
};

export const appendPhonesDemographicsRequest: IAppendMemberDemographicsRequest = {
  phones: [{ phone: '6159530693' }],
  updatedBy: '5b36fcd0-b5c6-4b98-a343-465107765420',
};

export const appendAddressesDemographicsRequest: IAppendMemberDemographicsRequest = {
  addresses: [{ street1: '1 Main St', city: 'Boston', state: 'MA', zip: '02101' }],
  updatedBy: '5b36fcd0-b5c6-4b98-a343-465107765420',
};

export const appendDemographicsRequest: IAppendMemberDemographicsRequest = {
  emails: [{ email: 'tdawg@gmail.com' }],
  phones: [{ phone: '6159530693' }],
  addresses: [{ street1: '1 Main St', city: 'Boston', state: 'MA', zip: '02101' }],
  updatedBy: '5b36fcd0-b5c6-4b98-a343-465107765420',
};

export const getMemberDemographicsRequest: IMemberDemographics = {
  firstName: 'Kong-sang',
  middleName: null,
  lastName: 'Chan',
  dateOfBirth: '1954-04-07',
  dateOfDemise: null,
  isMarkedDeceased: false,
  sex: 'male',
  gender: 'male',
  ethnicity: null,
  maritalStatus: 'domesticPartner',
  ssnLastFour: null,
  addresses: [
    {
      addressType: 'residential',
      city: 'Flushing',
      county: null,
      id: '9f777b6f-8715-4832-8ade-49d6d7c9dfaf',
      spanDateEnd: null,
      spanDateStart: null,
      state: 'NY',
      street1: '15 East Medows',
      street2: 'apt 7b',
      zip: '11392',
    },
  ],
  phones: [
    {
      id: '392fb165-e32b-4a1e-bac7-b1d58d56c6b8',
      phone: '7185526554',
      phoneType: 'main',
    },
    {
      id: '0664695e-d96e-4d46-9a2c-b94fcf806d08',
      phone: '5182329951',
      phoneType: 'main',
    },
  ],
  emails: [
    {
      email: 'bob@live.com',
      id: 'cea58957-55a2-411b-8f81-a2ffeb96347a',
    },
  ],
};

export const emptyMemberDemographicInfo: IMemberDemographics = {
  firstName: null,
  middleName: null,
  lastName: null,
  dateOfBirth: null,
  dateOfDemise: null,
  isMarkedDeceased: false,
  sex: null,
  gender: null,
  ethnicity: null,
  maritalStatus: null,
  updatedBy: null,
  ssnLastFour: null,
  addresses: [],
  phones: [],
  emails: [],
};

export const updateMemberZendeskIdRequest: IUpdateMemberRequest = {
  zendeskId: 'testZendeskId'
};

export const updateMemberContactRequest: IUpdateMemberRequest = {
  demographics: {
    addresses: [{
      street1: '180 Main St',
      street2: 'Apartment 1',
      city: 'Boston',
      state: 'MA',
      zip: '02105',
    }],
    phones: [{ phone: '3184529078', phoneType: 'mobile' }],
    emails: [{ email: 'jackiechan@gmail.com' }]
  }
};

export const updateMemberOnlyInsuranceRequest: IUpdateMemberRequest = {
  insurances: [{
      carrier: 'emblem',
      plans: [
        {
          externalId: 'M1',
          rank: 'primary',
          current: true,
          details: [{ lineOfBusiness: 'commercial', spanDateStart: '2019-01-01', spanDateEnd: '2021-03-01' }]
        },
        {
          externalId: 'MED1',
          rank: 'secondary',
          current: true,
          details: [{ lineOfBusiness: 'medicaid', spanDateStart: '2021-01-01' }]
        }
      ]
  }]
};

// tslint:disable-next-line: no-empty
test.skip('skip', () => {});
