import { IInsuranceDetails } from '../../models/member-insurance-details';
import { IInsurancePlan } from '../../models/member-insurance';
import { reduceInsurance } from '../insurance';

describe('Insurance Object Helpers', () => {
  describe('reduceInsurance', () => {
    const mockDetail: IInsuranceDetails = {
      lineOfBusiness: 'carefirst',
      subLineOfBusiness: 'medicaid',
      spanDateStart: null,
      spanDateEnd: null,
    };

    const mockInsurance: IInsurancePlan = {
      externalId: '11111',
      rank: '1',
    };

    const mockInsurancesMultipleLobs: IInsurancePlan[] = [
      {
        ...mockInsurance,
        details: [
          {
            ...mockDetail,
            spanDateStart: '2010-01-10',
            spanDateEnd: '2010-12-31',
          },
          {
            ...mockDetail,
            subLineOfBusiness: 'HARP',
            spanDateStart: '2009-01-01',
            spanDateEnd: '2009-01-31',
          },
          {
            ...mockDetail,
            spanDateStart: '2011-01-01',
            spanDateEnd: '2100-12-31',
          },
        ],
      },
    ];

    const mockInsurancesNullStartDate: IInsurancePlan[] = [
      {
        ...mockInsurance,
        details: [
          {
            ...mockDetail,
            subLineOfBusiness: 'HARP',
            spanDateEnd: '2009-01-31',
          },
          {
            ...mockDetail,
            spanDateStart: '2009-01-30',
            spanDateEnd: '2100-01-29',
          },
        ],
      },
    ];

    const mockInsurancesNullStartEndDate: IInsurancePlan[] = [
      {
        ...mockInsurance,
        details: [
          {
            ...mockDetail,
            subLineOfBusiness: 'Medicare',
          },
          {
            ...mockDetail,
            subLineOfBusiness: 'HARP',
            spanDateStart: '2009-01-27',
            spanDateEnd: '2100-01-29',
          },
          {
            ...mockDetail,
            subLineOfBusiness: 'HARP',
            spanDateStart: '2009-01-20',
            spanDateEnd: '2009-01-30',
          },
        ],
      },
    ];

    const mockInsurancesElibilityGaps: IInsurancePlan[] = [
      {
        ...mockInsurance,
        details: [
          {
            ...mockDetail,
            subLineOfBusiness: 'Medicaid',
            spanDateStart: '2009-01-20',
            spanDateEnd: '2009-01-25',
          },
          {
            ...mockDetail,
            subLineOfBusiness: 'HARP',
            spanDateStart: '2009-01-20',
            spanDateEnd: '2009-01-30',
          },
          {
            ...mockDetail,
            subLineOfBusiness: 'HARP',
            spanDateStart: '2009-03-01',
            spanDateEnd: '2010-01-29',
          },
        ],
      },
    ];

    const mockInsuranceReallyFarEndDates: IInsurancePlan[] = [
      {
        ...mockInsurance,
        details: [
          {
            ...mockDetail,
            subLineOfBusiness: 'HARP',
            spanDateStart: '2009-01-20',
            spanDateEnd: '2100-05-29',
          },
          {
            ...mockDetail,
            subLineOfBusiness: 'HARP',
            spanDateStart: '2009-01-27',
            spanDateEnd: '2100-01-29',
          },
        ],
      },
    ];

    it('collapses & canonicalizes insurances with multiple lines of business', () => {
      const reducedInsurances = reduceInsurance(mockInsurancesMultipleLobs);
      expect(reducedInsurances).toMatchObject([
        {
          ...mockInsurance,
          details: [
            {
              ...mockDetail,
              spanDateStart: '2010-01-10',
              spanDateEnd: null,
            },
            {
              ...mockDetail,
              subLineOfBusiness: 'HARP',
              spanDateStart: '2009-01-01',
              spanDateEnd: '2009-01-31',
            },
          ],
        },
      ]);
    });

    it('removes entries with null spanDateStart', () => {
      const reducedInsurances = reduceInsurance(mockInsurancesNullStartDate);
      expect(reducedInsurances).toMatchObject([
        {
          ...mockInsurance,
          details: [
            {
              ...mockDetail,
              spanDateStart: '2009-01-30',
              spanDateEnd: null,
            },
          ],
        },
      ]);
    });

    it('keeps entries with unique null spanDateStart & spanDateEnd', () => {
      const reducedInsurances = reduceInsurance(mockInsurancesNullStartEndDate);
      expect(reducedInsurances).toMatchObject([
        {
          ...mockInsurance,
          details: [
            {
              ...mockDetail,
              subLineOfBusiness: 'Medicare',
              spanDateStart: null,
              spanDateEnd: null,
            },
            {
              ...mockDetail,
              subLineOfBusiness: 'HARP',
              spanDateStart: '2009-01-20',
              spanDateEnd: null,
            },
          ],
        },
      ]);
    });

    it('allows span dates to have gaps in elibility without merging together the spans', () => {
      const reducedInsurances = reduceInsurance(mockInsurancesElibilityGaps);
      expect(reducedInsurances).toMatchObject([
        {
          ...mockInsurance,
          details: [
            {
              ...mockDetail,
              subLineOfBusiness: 'Medicaid',
              spanDateStart: '2009-01-20',
              spanDateEnd: '2009-01-25',
            },
            {
              ...mockDetail,
              subLineOfBusiness: 'HARP',
              spanDateStart: '2009-01-20',
              spanDateEnd: '2009-01-30',
            },
            {
              ...mockDetail,
              subLineOfBusiness: 'HARP',
              spanDateStart: '2009-03-01',
              spanDateEnd: '2010-01-29',
            },
          ],
        },
      ]);
    });

    it('allows for no insurances to be processed & not break as a result', () => {
      const reducedInsurances = reduceInsurance([]);
      expect(reducedInsurances).toMatchObject([]);
    });

    it('correctly reduces insurance objects with spanDateStart & spanDateEnd really far into the future', () => {
      const reducedInsurances = reduceInsurance(mockInsuranceReallyFarEndDates);
      expect(reducedInsurances).toMatchObject([
        {
          ...mockInsurance,
          details: [
            {
              ...mockDetail,
              subLineOfBusiness: 'HARP',
              spanDateStart: '2009-01-20',
              spanDateEnd: null,
            },
          ],
        },
      ]);
    });
  });
});
