import * as KafkaFactoryModule from "../../src/lib/KafkaFactory";
import { KafkaStreams } from '../../src/lib/KafkaStreams';
import KafkaFactoryStub from '../utils/KafkaFactoryStub';

// const { KafkaStreams } = proxyquire('../../src/lib/KafkaStreams', {
//   './KafkaFactory': { KafkaFactory: KafkaFactoryStub }
// });

describe('Join UNIT', () => {

  describe('KStream <-> KStream', () => {
    afterEach(() => {
      // restore the spy created with spyOn
      jest.restoreAllMocks();
    });
    it('should be able to inner join kstreams', (done) => {
      jest.spyOn(KafkaFactoryModule, 'KafkaFactory').mockImplementation(((config, batchOptions = undefined) => {
        return new KafkaFactoryStub();
      }) as any);
      const streams = new KafkaStreams({});
      const factory = streams.factory;

      const parent = streams.getKStream(null);
      parent
        .mapStringToKV()
        .map(event => {
          if (event && event.key == 'other') {
            event.otherKey = event.key;
          }
          return event;
        });

      const side = streams.getKStream(null);
      side
        .mapStringToKV()
        .map(event => {
          if (event && event.key == 'other') {
            event.otherKey = event.key;
          }
          return event;
        });

      const joined = parent.innerJoin(side, 'otherKey');
      joined.to('some-output-topic');

      const parentMessages = [
        'other x1',
        null,
        '',
        undefined,
        'other x2',
        'else',
        'whatelse',
        'other x3'
      ];

      const sideMessages = [
        'wut',
        'wat',
        null,
        '',
        undefined,
        'other x1',
        'else'
      ];

      setTimeout(() => {
        side.writeToStream('other x2');
        side.writeToStream('other x3');
      }, 15);

      parentMessages.forEach(m => parent.writeToStream(m));
      sideMessages.forEach(m => side.writeToStream(m));

      setTimeout(() => {
        console.log(factory);
        const messages = factory.lastProducer.producedMessages;
        console.log(messages);
        expect(messages[0].left.value).toBe('x1');
        expect(messages[0].left.value).toBe(messages[0].right.value);

        expect(messages[1].left.value).toBe('x2');
        expect(messages[1].left.value).toBe(messages[1].right.value);

        expect(messages[2].left.value).toBe('x3');
        expect(messages[2].left.value).toBe(messages[2].right.value);

        done();
      }, 20);
    });
  });
});
