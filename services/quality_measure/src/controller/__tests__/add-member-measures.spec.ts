import { Request, Response } from 'express';
import { addMemberMeasures } from '../add-member-measures';

const mockResponse = () => {
  const res = ({} as any) as Response;
  res.status = jest.fn().mockReturnValue(res);
  res.json = jest.fn().mockReturnValue(res);
  return res;
};

describe('add-member-measure controller', () => {
  describe('addMemberMeasures', () => {
    it('throws error if memberId parameter is an invalid uuid', async () => {
      const req = ({
        params: {
          memberId: '',
        },
      } as any) as Request;

      const res = mockResponse();

      await addMemberMeasures(req, res);
      expect(res.status).toHaveBeenCalledWith(400);
      expect(res.json).toHaveBeenCalledWith({
        error: 'Invalid uuid provided as memberId parameter',
      });
    });
  });
});
