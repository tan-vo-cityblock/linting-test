import { chain, toArray, groupBy, isEmpty, isNil, pick, sortBy, reduce } from 'lodash';
import { IInsuranceDetails } from '../models/member-insurance-details';
import { IInsurance, IInsurancePlan } from '../models/member-insurance';

export interface ILinesOfBusiness {
  lineOfBusiness: string;
  subLineOfBusiness: string;
}

export function reduceInsurance(insurances: IInsurancePlan[]) {
  if (insurances.length < 1) {
    return insurances;
  } else {
    // We cannot have aspan date with a non-null end and a null start
    const normalizedInsurances: IInsurancePlan[] = insurances.map((insurance: IInsurancePlan) => {
      const mappedDetails: IInsuranceDetails[] | null = insurance.details && insurance.details.map(canonicalizeSpanEndDates);
      return {
        ...insurance,
        details: groupSpansInsurances(mappedDetails),
      };
    });
    return normalizedInsurances;
  }
}

// TODO: if spanDateEnd is null, the code breaks... if spanDateEnd is 2199, the code breaks
function canonicalizeSpanEndDates(insurance: IInsuranceDetails) {
  const twentyYearsFromNow = (): Date => {
    const currentDate = new Date();
    currentDate.setFullYear(currentDate.getFullYear() + 20);
    return currentDate;
  };
  if (new Date(insurance.spanDateEnd) > twentyYearsFromNow()) {
    return { ...insurance, spanDateEnd: null };
  } else {
    return insurance;
  }
}

function groupSpansInsurances(currentInsurances: IInsuranceDetails[] | null) {
  if (isEmpty(currentInsurances)) {
    return [];
  }

  const groupedInsurances = groupLobSlob(currentInsurances);
  let reducedFlattenedInsurances: IInsuranceDetails[] = [];
  for (const key of Object.keys(groupedInsurances)) {
    // We cannot pass through a span date with a non-null end and a null start through the reducer
    const hasStartDate = (insurance: IInsuranceDetails) => !isNil(insurance.spanDateStart);
    const hasNoStartNorEndDate = (insurance: IInsuranceDetails) =>
      isNil(insurance.spanDateStart) && isNil(insurance.spanDateEnd);

    const insurancesWithStartDates = groupedInsurances[key].filter((insurance) =>
      hasStartDate(insurance),
    );
    const insurancesWithNoSpanDates = groupedInsurances[key].filter((insurance) =>
      hasNoStartNorEndDate(insurance),
    );
    // We can have a null start and null end, BUT, we only want it as the ONLY data point we have for the combination
    const insurancesToAppend =
      insurancesWithStartDates.length == 0
        ? insurancesWithNoSpanDates
        : spanReducer(insurancesWithStartDates);
    reducedFlattenedInsurances = reducedFlattenedInsurances.concat(insurancesToAppend);
  }
  return reducedFlattenedInsurances;
}

export function groupLobSlob(insurances: Partial<IInsuranceDetails>[]) {
  return groupBy(
    insurances,
    (insurance) => insurance.lineOfBusiness + insurance.subLineOfBusiness,
  );
}

function spanReducer(currentInsurances: IInsuranceDetails[]) {
  const sortedInsurances = sortBy(currentInsurances, (insurance) =>
    new Date(insurance.spanDateStart).getTime(),
  );
  if (sortedInsurances.length < 2) {
    return sortedInsurances;
  }
  return reduce(sortedInsurances.slice(1), spanIterator, [sortedInsurances[0]]);
}

function spanIterator(finalInsurances: IInsuranceDetails[], insurance: IInsuranceDetails) {
  // If spanDateStart == 2020-02-01 & spanDateEnd == 2020-01-31 (i.e., seems illogical), this means service ends 2020-01-31 (Tufts, Carefirst)
  // Should be taken care of by attribution, if not something crazy happened
  if (dateDifferenceInDays(insurance.spanDateEnd, insurance.spanDateStart) < 0) {
    throw new Error(`[spanDateStart: ${insurance.spanDateStart}] for insurance end is greater than [spanDateStart: ${insurance.spanDateEnd}]`)
  } else if (
    dateDifferenceInDays(
      finalInsurances[finalInsurances.length - 1].spanDateEnd,
      insurance.spanDateStart,
    ) > -2
  ) {
    insurance.spanDateStart = finalInsurances[finalInsurances.length - 1].spanDateStart;
    finalInsurances.pop();
    finalInsurances.push(insurance);
  } // If span date interval is not connected
  else {
    finalInsurances.push(insurance);
  }
  return finalInsurances;
}

const maxDate = 8640000000000000;

function dateDifferenceInDays(dateTo: string | null | undefined, dateFrom: string) {
  const maxTimestamp = new Date(maxDate);
  const newDateFrom = new Date(dateFrom).getTime();
  const newDateTo = isNil(dateTo) ? maxTimestamp.getTime() : new Date(dateTo).getTime();
  return Math.ceil(Number(newDateTo - newDateFrom) / (1000 * 3600 * 24));
}

export function processInsurance(mdiSubQuery: any[]): IInsurance[] {
  const insurances = chain(mdiSubQuery)
    .groupBy('carrier')
    .map((plans, carrier: string) => {
      return { carrier, plans: processInsurancePlans(plans) };
    });

  return toArray(insurances);
}

export function processInsurancePlans(memberInsurances: any[]): IInsurancePlan[] {
  return memberInsurances.map(memberInsurance => {
    return {
      externalId: memberInsurance.externalId,
      rank: memberInsurance.rank,
      current: memberInsurance.current,
      details: processInsuranceDetails(memberInsurance.details)
    };
  });
}

export function processInsuranceDetails(insuranceDetails: any[]): IInsuranceDetails[] {
  return insuranceDetails.map(details =>
    pick(details, ['id', 'lineOfBusiness', 'subLineOfBusiness', 'spanDateStart', 'spanDateEnd'])
  );
}
