import { Request, Response } from 'express';
import { MemberEligibilities } from '../models/member-eligibilities';

export async function getEligibilities(request: Request, response: Response) {
  const { memberId } = request.params;
  const eligibilities = await MemberEligibilities.getForMember(memberId);
  return response.send(eligibilities);
}
