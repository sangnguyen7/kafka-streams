import * as KafkaFactoryModule from "../../src/lib/KafkaFactory";
import { KafkaStreams } from '../../src/lib/KafkaStreams';
import KafkaFactoryStub from '../utils/KafkaFactoryStub';

describe("KTable UNIT", () => {
    afterEach(() => {
        // restore the spy created with spyOn
        jest.restoreAllMocks();
    });
    it("should be able to represent a table from a stream", (done) => {
        jest.spyOn(KafkaFactoryModule, 'KafkaFactory').mockImplementation(((config, batchOptions = undefined) => {
            return new KafkaFactoryStub();
        }) as any);
        function etl_KeyValueMapper (message) {
            const elements = message.toLowerCase().split(" ");
            return {
                key: elements[0],
                value: elements[1]
            };
        }

        let intv = null;
        let count = 0;
        let hit = 0;
        let hitCount = 0;

        const streams = new KafkaStreams({});
        const factory = streams.factory;
        const source = streams.getKTable("ktable-unit", etl_KeyValueMapper);

        source
            .tap(_ => {
                count++;
            })
            .consumeUntilCount(21, () => {

                expect(count).toBe(21);
                expect(hit).toBe(1);
                expect(hitCount - 5 >= 0).toBe(true);

                const messages = factory.lastProducer.producedMessages;
                //console.log(messages);

                source.getTable().then((data: any) => {

                    console.log(data);

                    expect(data.derp).toBe("7");
                    expect(data.derpa).toBe("7");
                    expect(data.derpb).toBe("7");

                    const replays = {} as any;

                    source.forEach(kv => {
                        console.log(kv);
                        replays[kv.key] = kv.value;
                        if (Object.keys(replays).length === 3) {

                            expect(replays.derp).toBe("7");
                            expect(replays.derpa).toBe("7");
                            expect(replays.derpb).toBe("7");

                            streams.closeAll();
                            clearInterval(intv);
                            done();
                        }
                    });

                    source.replay();
                });
            })
            .atThroughput(5, () => {
                hit++;
                hitCount = count;
            })
            .tap(console.log)
            .to("streams-wordcount-output");

        source.start();

        let intervalCount = 0;
        intv = setInterval(() => {
            intervalCount++;
            factory.lastConsumer.fakeIncomingMessages([
                "derpa " + intervalCount, "derp " + intervalCount, "derpb " + intervalCount
            ]);
        }, 2);
    });
});
