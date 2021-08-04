import { find } from '../models/hie-message-signal';

const resolvers = {
  HieMessageSignal: {
    __resolveReference: async (reference: { __typename: string; messageId: string | null }) => {
      if (!reference.messageId) return null;
      return find(reference.messageId);
    },
  },
};

export default resolvers;
