import { Request, Response } from 'express';
import { MemberEligibilities } from '../models/member-eligibilities';

export async function updateEligibilities(request: Request, response: Response) {
  const { memberId } = request.params;
  const eligibilities = await MemberEligibilities.upsertForMember(memberId, request.body);
  return response.send(eligibilities);
}
