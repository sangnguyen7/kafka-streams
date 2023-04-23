import * as KafkaFactoryModule from "../../src/lib/KafkaFactory";
import { KafkaStreams } from '../../src/lib/KafkaStreams';
import KafkaFactoryStub from '../utils/KafkaFactoryStub';

describe("WordCount UNIT", () => {
    afterEach(() => {
        // restore the spy created with spyOn
        jest.restoreAllMocks();
    });
    it("should be able to count words", (done) => {
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

        function etl_deflate (value) {
            return value.count;
        }

        const streams = new KafkaStreams({});
        const factory = streams.factory;
        const source = streams.getKStream("word-count-unit");

        source
            .map(etl_ValueFlatten)
            .map(etl_KeyValueMapper)
            .countByKey("key", "count")
            .skip(7)
            .take(3)
            .map(etl_deflate)
            .to("streams-wordcount-output");

        source.start();

        factory.lastConsumer.fakeIncomingMessages([
            "if bla", "if xta", "bla 1", "if blup",
            "blup 2", "if hihi", "bla 2", "if five",
            "bla third", "blup second derp"
        ]); // if = 5, bla = 3, blup = 2

        setTimeout(() => {
            const messages = factory.lastProducer.producedMessages;
            console.log(messages);

            expect(messages[0]).toBe(5); //if
            expect(messages[1]).toBe(3); //bla
            expect(messages[2]).toBe(2); //blup

            streams.closeAll();
            done();
        }, 5);
    });
});
