import * as KafkaFactoryModule from "../../src/lib/KafkaFactory";
import { KafkaStreams } from '../../src/lib/KafkaStreams';
import KafkaFactoryStub from '../utils/KafkaFactoryStub';

describe("Sum-Action UNIT", function () {
    afterEach(() => {
        // restore the spy created with spyOn
        jest.restoreAllMocks();
    });
    it("should be able to sum values", function (done) {
        jest.spyOn(KafkaFactoryModule, 'KafkaFactory').mockImplementation(((config, batchOptions = undefined) => {
            return new KafkaFactoryStub();
        }) as any);
        function etl_ValueFlatten (value) {
            return value.toLowerCase().split(" ");
        }

        function etl_KeyValueMapper (elements) {
            return {
                key: elements[0],
                value: elements[1]
            };
        }

        const streams = new KafkaStreams({});
        const factory = streams.factory;
        const source = streams.getKStream("sum-action-unit");

        source
            .map(etl_ValueFlatten)
            .map(etl_KeyValueMapper)
            .take(11)
            .sumByKey("key", "value", true)
            .skip(7)
            .map(kv => kv.sum)
            .to("streams-wordcount-output");

        source.start();

        factory.lastConsumer.fakeIncomingMessages([
            "abc 1", "def 1", "abc 3", "fus eins,", "def 4",
            "abc 12", "fus zwei,", "def 100", "abc 50", "ida 0",
            "fus drei"
        ]); // abc 66, def 105, ida 0, fus eins,zwei,drei

        setTimeout(() => {
            const messages = factory.lastProducer.producedMessages;
            console.log(messages);

            const data = source.storage.state;

            expect(data.abc).toBe(66);
            expect(data.def).toBe(105);
            expect(data.ida).toBe(0);
            expect(data.fus).toBe("eins,zwei,drei");

            streams.closeAll();
            done();
        }, 5);
    });
});
