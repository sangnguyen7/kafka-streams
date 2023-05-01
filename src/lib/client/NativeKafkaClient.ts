/* eslint-disable @typescript-eslint/no-empty-function */
import uuid from "uuid";
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
//@ts-ignore
import debugFactory from "debug";
const debug = debugFactory("kafka-streams:nativeclient");
import { KafkaClient } from "./KafkaClient";
import JSConsumer from "./JSConsumer";
import { JSProducer } from "./JSProducer";

const NOOP = () => { };

export class NativeKafkaClient extends KafkaClient {
	public topic: any;
	public config: any;
	public batchOptions: any;
	public consumer: any;
	public producer: any;
	public produceTopic: any;
	public producePartitionCount: any;
	public _produceHandler: any;

	/**
	 * NativeKafkaClient (EventEmitter)
	 * that wraps an internal instance of a
	 * Sinek native kafka- Consumer and/or Producer
	 * @param topic
	 * @param config
	 * @param batchOptions - optional
	 */
	constructor (topic, config, batchOptions = undefined) {
		super();

		this.topic = topic;
		this.config = config;
		this.batchOptions = batchOptions;

		this.consumer = null;
		this.producer = null;

		this.produceTopic = null;
		this.producePartitionCount = 1;
		this._produceHandler = null;
	}

	/**
	 * sets a handler for produce messages
	 * (emits whenever kafka messages are produced/delivered)
	 * @param handler {EventEmitter}
	 */
	setProduceHandler (handler) {
		this._produceHandler = handler;
	}

	/**
	 * returns the produce handler instance if present
	 * @returns {null|EventEmitter}
	 */
	getProduceHandler () {
		return this._produceHandler;
	}

	/**
	 * overwrites the topic
	 * @param topics {Array<string>}
	 */
	overwriteTopics (topics) {
		this.topic = topics;
	}

	adjustDefaultPartitionCount (partitionCount = 1) {
		this.producePartitionCount = partitionCount;
		this.producer.defaultPartitionCount = partitionCount;
	}

	/**
	 * starts a new kafka consumer
	 * will await a kafka-producer-ready-event if started withProducer=true
	 * @param readyCallback
	 * @param kafkaErrorCallback
	 * @param withProducer
	 * @param withBackPressure
	 */
	start (readyCallback = null, kafkaErrorCallback = null, withProducer = false, withBackPressure = false) {

		//might be possible if the parent stream is build to produce messages only
		if (!this.topic || !this.topic.length) {
			return;
		}

		//passing batch options will always result in backpressure mode
		if (this.batchOptions) {
			withBackPressure = false;
		}

		kafkaErrorCallback = kafkaErrorCallback || NOOP;

		this.consumer = new JSConsumer(this.topic, this.config);

		this.consumer.on("ready", readyCallback || NOOP);
		this.consumer.on("error", kafkaErrorCallback);

		//consumer has to wait for producer
		super.once("kafka-producer-ready", async () => {

			const streamOptions = {
				asString: false,
				asJSON: false
			};

			//if backpressure is desired, we cannot connect in streaming mode
			//if it is not we automatically connect in stream mode
			try {
				await this.consumer.connect(false, streamOptions)
				debug("consumer ready");
				if (withBackPressure) {
					debug('withBackPressure');
					await this.consumer.consume((message, done) => {
						debug('[withBackPressure]Consuming message: ' + message);
						super.emit("message", message);
						done();
					}, false, false, this.batchOptions);
				} else {
					debug('NoBackPressure');
					this.consumer.on("message", message => {
						debug('Consuming message: ' + message);
						super.emit("message", message);
					});
					await this.consumer.consume();
				}
			} catch (error) {
				kafkaErrorCallback(error)
			}
		});

		if (!withProducer) {
			super.emit("kafka-producer-ready", true);
		}
	}

	/**
	 * starts a new kafka-producer
	 * will fire kafka-producer-ready-event
	 * requires a topic's partition count during initialisation
	 * @param produceTopic
	 * @param partitions
	 * @param readyCallback
	 * @param kafkaErrorCallback
	 * @param outputKafkaConfig
	 */
	async setupProducer (produceTopic, partitions = 1, readyCallback = null, kafkaErrorCallback = null, outputKafkaConfig = null) {

		this.produceTopic = produceTopic || this.produceTopic;
		this.producePartitionCount = partitions;

		kafkaErrorCallback = kafkaErrorCallback || NOOP;

		const config = outputKafkaConfig || this.config;

		//might be possible if the parent stream is build to produce messages only
		if (!this.producer) {
			// Sinek library has some strange null value here so we map this to any
			this.producer = new JSProducer(config, [this.produceTopic] as any, this.producePartitionCount);

			//consumer is awaiting producer
			this.producer.on("ready", () => {
				debug("producer ready");
				super.emit("kafka-producer-ready", true);
				if (readyCallback) {
					readyCallback();
				}
			});

			this.producer.on("error", kafkaErrorCallback);
			try {
				await this.producer.connect()
			} catch (error) {
				kafkaErrorCallback(error)
			}
		}
	}

	//async send(topicName, message, _partition = null, _key = null, _partitionKey = null, _opaqueKey = null)

	/**
	 * simply produces a message or multiple on a topic
	 * if producerPartitionCount is > 1 it will randomize
	 * the target partition for the message/s
	 * @param topicName
	 * @param message
	 * @param partition - optional
	 * @param key - optional
	 * @param partitionKey - optional
	 * @param opaqueKey - optional
	 * @param {array} headers - optional array with message headers (key/value objects)
	 * @returns {Promise<void>}
	 */
	async send (topicName, message, partition = null, key = null, partitionKey = null, opaqueKey = null, headers = null) {

		if (!this.producer) {
			throw new Error("producer is not yet setup.");
		}

		return await this.producer.send(topicName, message, partition, key, partitionKey, headers);
	}

	/**
	 * buffers a keyed message to be send
	 * a keyed message needs an identifier, if none is provided
	 * an uuid.v4() will be generated
	 * @param topic
	 * @param identifier
	 * @param payload
	 * @param _ - optional
	 * @param partition - optional
	 * @param version - optional
	 * @param partitionKey - optional
	 * @returns {Promise<void>}
	 */
	buffer (topic, identifier, payload, _ = null, partition = null, version = null, partitionKey = null) {

		if (!this.producer) {
			return Promise.reject("producer is not yet setup.");
		}

		return this.producer.buffer(topic, identifier, payload, partition, version, partitionKey);
	}

	/**
	 * buffers a keyed message in (a base json format) to be send
	 * a keyed message needs an identifier, if none is provided
	 * an uuid.4() will be generated
	 * @param topic
	 * @param identifier
	 * @param payload
	 * @param version - optional
	 * @param _ - optional
	 * @param partitionKey - optional
	 * @param partition - optional
	 * @returns {Promise<void>}
	 */
	bufferFormat (topic, identifier, payload, version = 1, _ = null, partitionKey = null, partition = null) {

		if (!this.producer) {
			return Promise.reject("producer is not yet setup.");
		}

		if (!identifier) {
			identifier = uuid.v4();
		}

		return this.producer.bufferFormatPublish(topic, identifier, payload, version, undefined, partitionKey, partition);
	}

	pause () {

		//no consumer pause

		if (this.producer) {
			this.producer.pause();
		}
	}

	resume () {

		//no consumer resume

		if (this.producer) {
			this.producer.resume();
		}
	}

	getStats () {
		return {
			inTopic: this.topic ? this.topic : null,
			consumer: this.consumer ? this.consumer.getStats() : null,

			outTopic: this.produceTopic ? this.produceTopic : null,
			producer: this.producer ? this.producer.getStats() : null
		};
	}

	close (commit = false) {

		if (this.consumer) {
			this.consumer.close(commit);
		}

		if (this.producer) {
			this.producer.close();
		}
	}

	//required by KTable
	closeConsumer (commit = false) {

		if (this.consumer) {
			this.consumer.close(commit);
			this.consumer = null;
		}
	}
}
