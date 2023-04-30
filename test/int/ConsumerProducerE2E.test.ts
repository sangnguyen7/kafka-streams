import { KafkaStreams } from "../../src/index";
import { nativeConfig as config } from "../test-config";
import debugFactory from "debug";
const debug = debugFactory("kafka-streams:test:int");

const keyValueMapperEtl = (message) => {
  debug(message);
  const elements = message.toLowerCase().split(" ");
  return {
    key: elements[0],
    value: elements[1]
  };
};

/*
  E2E or integration tests using a kafka broker are always
  a bit flakey, with the right configuration and enough patience (mocha timeouts)
  it is relatively possible.
 */

describe("E2E INT", () => {

  let kafkaStreams = null;

  const topic = "my-input-topic";
  const outputTopic = "my-output-topic";

  const messages = [
    "bla",
    "blup",
    "bluuu",
    "bla",
    "bla",
    "blup",
    "xd",
    "12x3"
  ];

  beforeAll(() => {
    kafkaStreams = new KafkaStreams(config);
  });

  afterAll(async () => {
    await kafkaStreams.closeAll();
  });

  it("should be able to produce to a topic via stream", done => {

    const stream = kafkaStreams.getKStream();
    stream.to(topic);

    let count = 0;
    stream.createAndSetProduceHandler().on("delivered", message => {
      debug("delivered", message.value);
      count++;
      if (count === messages.length) {
        setTimeout(done, 250);
      }
    });

    stream.start().then(() => {
      debug("started");
      stream.writeToStream(messages);
      done();
    }).catch((error) => {
      done(error);
    });
  });

  it("should give kafka some time", done => {
    setTimeout(done, 9000);
  }, 10000);

  it("should run complexer wordcount sample", done => {

    const stream = kafkaStreams.getKStream(topic);

    let count = 0;
    stream.createAndSetProduceHandler().on("delivered", (message) => {
      debug("delivered", message);
      done();
      count++;
      if (count === 2) {
        setTimeout(done, 250);
      }
    });

    stream.start().then(() => {
      debug("consumed started");
      stream
        .from(topic)
        .mapJSONConvenience() //buffer -> json
        .mapWrapKafkaValue() //message.value -> value
        .map(keyValueMapperEtl)
        .countByKey("key", "count")
        .filter(kv => kv.count >= 2)
        .map(kv => kv.key + " " + kv.count)
        .tap(_ => { })
        .wrapAsKafkaValue()
        .to(outputTopic);
    });
  }, 15000);

  // it("should give kafka some time again", done => {
  //   setTimeout(done, 2500);
  // });

  // it("should be able to consume produced wordcount results", done => {

  //   const stream = kafkaStreams.getKStream();

  //   let count = 0;
  //   stream
  //     .from(outputTopic)
  //     .mapJSONConvenience() //buffer -> json
  //     .tap(_ => {
  //       count++;
  //       if (count === 2) {
  //         setTimeout(done, 100);
  //       }
  //     })
  //     .forEach(debug);

  //   stream.start();
  // });
});
