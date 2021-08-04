import validate from 'uuid-validate';

export function isTruthyString(str: any) {
  return !!str && typeof str === 'string';
}

export function isTruthyNumber(num: any) {
  return !!num && typeof num === 'number';
}

export function isTruthyNumericString(str: any) {
  return isTruthyString(str) && !!Number(str);
}

export function isValidUuid(str: any) {
  return isTruthyString(str) && validate(str);
}

export function isDateInRangeExclusive(start: Date, datetime: Date, end: Date) {
  return start.getTime() <= datetime.getTime() && datetime.getTime() < end.getTime();
}
