import { EventEmitter } from "events";
import * as most from "most";

class FakeKafka extends EventEmitter {

    constructor () {
        super();
    }

    fake () {
        this.emit("message", "mY value");
        this.emit("message", "mY value2");
        this.emit("message", "JuRi value1");
        this.emit("message", "mY value3");
        this.emit("message", "JuRi value2");
    }
}

describe("Observable UNIT", () => {

    it("should be able to observe", (done) => {

        const countMap = {};
        function slowKeyCount (value) {
            return new Promise(resolve => {

                if (countMap[value.key]) {
                    countMap[value.key]++;
                } else {
                    countMap[value.key] = 1;
                }

                resolve({
                    key: value.key,
                    value: countMap[value.key]
                });
            });
        }

        const kafka = new FakeKafka();

        const stream$ = most.fromEvent("message", kafka)
            .map((value: any) => value.toLowerCase().split(" "))
            .map(value => ({ key: value[0], "value": value[1] }))
            .flatMap(value => most.fromPromise(slowKeyCount(value)))
            .recoverWith(err => {
                console.log(err);
                return most.empty();
            });

        stream$.forEach(value => {
            console.log(value);
        });

        setTimeout(() => {
            kafka.fake();
            done();
        }, 100);
    });
});
