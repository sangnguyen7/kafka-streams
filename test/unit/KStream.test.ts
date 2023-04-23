import * as KafkaFactoryModule from "../../src/lib/KafkaFactory";
import { KafkaStreams } from '../../src/lib/KafkaStreams';
import KafkaFactoryStub from '../utils/KafkaFactoryStub';

describe("KStream UNIT", () => {

    describe("KStream branching", () => {
        afterEach(() => {
            // restore the spy created with spyOn
            jest.restoreAllMocks();
        });
        it("should be able to branch kstream into kstreams", (done) => {
            jest.spyOn(KafkaFactoryModule, 'KafkaFactory').mockImplementation(((config, batchOptions = undefined) => {
                return new KafkaFactoryStub();
            }) as any);
            const streams = new KafkaStreams({});

            const parent = streams.getKStream(null);

            const [
                streamA,
                streamB,
                streamTrue
            ] = parent.branch([
                (message) => message.startsWith("a"),
                (message) => message.startsWith("b"),
                (message) => !!message
            ]);

            const outputA = [];
            streamA.forEach((a) => outputA.push(a));

            const outputB = [];
            streamB.forEach((b) => outputB.push(b));

            const outputTrue = [];
            streamTrue.forEach((t) => outputTrue.push(t));

            const outputParent = [];
            parent.forEach((p) => outputParent.push(p));

            const parentMessages = [
                "albert",
                "bunert",
                "brabert",
                "anna",
                "anne",
                "ansgar",
                "carsten",
                "beter",
                "christina",
                "bolf",
                "achim"
            ];

            setTimeout(() => {
                parent.writeToStream("alina");
                parent.writeToStream("bela");
            }, 15);

            parentMessages.forEach(m => parent.writeToStream(m));

            setTimeout(() => {

                expect(outputA.length).toBe(6);
                expect(outputB.length).toBe(5);
                expect(outputTrue.length).toBe(parentMessages.length + 2);
                expect(outputParent.length).toBe(parentMessages.length + 2);

                done();
            }, 20);
        });
    });
});
