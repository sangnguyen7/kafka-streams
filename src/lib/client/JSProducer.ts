"use strict";
import debug from "debug";
import EventEmitter from "events";
import { Kafka } from "kafkajs";
import fs from 'fs';
import { v4 as uuidv4 } from 'uuid';
import murmur2Partitioner from 'murmur2-partitioner';
import murmur from "murmurhash";
import Metadata from "./metadata";
import { ProducerHealth } from "./health";
import { ProducerAnalytics } from "./analytics";

const MESSAGE_TYPES = {
    PUBLISH: "-published",
    UNPUBLISH: "-unpublished",
    UPDATE: "-updated"
};

const MAX_PART_AGE_MS = 1e3 * 60 * 5; //5 minutes
const MAX_PART_STORE_SIZE = 1e4;
const DEFAULT_MURMURHASH_VERSION = "3";

const DEFAULT_LOGGER = {
    debug: debug("sinek:jsproducer:debug"),
    info: debug("sinek:jsproducer:info"),
    warn: debug("sinek:jsproducer:warn"),
    error: debug("sinek:jsproducer:error")
};

/**
 * native producer wrapper for node-librdkafka
 * @extends EventEmitter
 */
export class JSProducer extends EventEmitter {
    kafkaClient: Kafka;
    config: any;
    private _health: any;
    private _adminClient: any;
    paused: boolean;
    producer: any;
    private _producerPollIntv: any;
    defaultPartitionCount: number | string;
    private _partitionCounts: any;
    private _inClosing: boolean;
    private _totalSentMessages: number;
    private _lastProcessed: any;
    private _analyticsOptions: any;
    private _analyticsIntv: any;
    private _analytics: any;
    private _murmurHashVersion: any;
    private _murmur: (key: any, partitionCount: any) => any;
    private _errors: number;

    /**
     * creates a new producer instance
     * @param {object} config - configuration object
     * @param {*} _ - ignore this param (api compatability)
     * @param {number|string} defaultPartitionCount  - amount of default partitions for the topics to produce to
     */
    constructor (config: any = { options: {}, health: {} }, _, defaultPartitionCount = 1) {
        super();

        if (!config) {
            throw new Error("You are missing a config object.");
        }

        if (!config.logger || typeof config.logger !== "object") {
            config.logger = DEFAULT_LOGGER;
        }

        if (!config.options) {
            config.options = {};
        }

        if (!config.noptions) {
            config.noptions = {};
        }

        const brokers = config.kafkaHost ||
            (config.noptions["metadata.broker.list"] && config.noptions["metadata.broker.list"].split(","));
        const clientId = config.noptions["client.id"];

        if (!brokers || !clientId) {
            throw new Error("You are missing a broker or group configs");
        }

        if (config.noptions["security.protocol"]) {
            this.kafkaClient = new Kafka({
                brokers,
                clientId,
                ssl: {
                    ca: [fs.readFileSync(config.noptions["ssl.ca.location"], "utf-8")],
                    cert: fs.readFileSync(config.noptions["ssl.certificate.location"], "utf-8"),
                    key: fs.readFileSync(config.noptions["ssl.key.location"], "utf-8"),
                    passphrase: config.noptions["ssl.key.password"],
                },
                sasl: {
                    mechanism: config.noptions["sasl.mechanisms"],
                    username: config.noptions["sasl.username"],
                    password: config.noptions["sasl.password"],
                },
            });
        } else {
            this.kafkaClient = new Kafka({ brokers, clientId });
        }

        this.config = config;
        this._health = new ProducerHealth(this, this.config.health || {});
        this._adminClient = this.kafkaClient.admin();

        this.paused = false;
        this.producer = null;
        this._producerPollIntv = null;
        this.defaultPartitionCount = defaultPartitionCount;
        this._partitionCounts = {};
        this._inClosing = false;
        this._totalSentMessages = 0;
        this._lastProcessed = null;
        this._analyticsOptions = null;
        this._analyticsIntv = null;
        this._analytics = null;

        this._murmurHashVersion = this.config.options.murmurHashVersion || DEFAULT_MURMURHASH_VERSION;
        this.config.logger.info(`using murmur ${this._murmurHashVersion} partitioner.`);

        switch (this._murmurHashVersion) {
            case "2":
                this._murmur = (key, partitionCount) => murmur2Partitioner.partition(key, partitionCount);
                break;

            case "3":
                this._murmur = (key, partitionCount) => murmur.v3(key) % partitionCount;
                break;

            default:
                throw new Error(`${this._murmurHashVersion} is not a supported murmur hash version. Choose '2' or '3'.`);
        }

        this._errors = 0;
        super.on("error", () => this._errors++);
    }

    /**
     * @throws
     * starts analytics tasks
     * @param {object} options - analytic options
     */
    enableAnalytics (options: any = {}) {

        if (this._analyticsIntv) {
            throw new Error("analytics intervals are already running.");
        }

        let {
            analyticsInterval
        } = options;
        this._analyticsOptions = options;

        analyticsInterval = analyticsInterval || 1000 * 150; // 150 sec

        this._analyticsIntv = setInterval(this._runAnalytics.bind(this), analyticsInterval);
    }

    /**
     * halts all analytics tasks
     */
    haltAnalytics () {

        if (this._analyticsIntv) {
            clearInterval(this._analyticsIntv);
        }
    }

    /**
     * connects to the broker
     * @returns {Promise.<*>}
     */
    async connect () {
        // eslint-disable-next-line prefer-const
        let { zkConStr, kafkaHost, logger, options, noptions, tconf
        } = this.config;

        const {
            pollIntervalMs
        } = options;

        let conStr = null;

        if (typeof kafkaHost === "string") {
            conStr = kafkaHost;
        }

        if (typeof zkConStr === "string") {
            conStr = zkConStr;
        }

        if (conStr === null && !noptions) {
            throw new Error("One of the following: zkConStr or kafkaHost must be defined.");
        }

        if (conStr === zkConStr) {
            throw new Error("NProducer does not support zookeeper connection.");
        }

        const config = {
            "metadata.broker.list": conStr,
            "dr_cb": true
        };

        noptions = noptions || {};
        noptions = Object.assign({}, config, noptions);
        logger.debug(noptions);

        tconf = tconf ? tconf : {
            "request.required.acks": 1
        };
        logger.debug(tconf);

        this.producer = this.kafkaClient.producer();
        const { CONNECT, DISCONNECT, REQUEST_TIMEOUT } = this.producer.events;

        this.producer.on(REQUEST_TIMEOUT, details => {
            super.emit("error", new Error(`Request Timed out. Info ${JSON.stringify(details)}`));
        });

        /* ### EOF STUFF ### */

        this.producer.on(DISCONNECT, () => {
            if (this._inClosing) {
                this._reset();
            }
            logger.warn("Disconnected.");
            //auto-reconnect??? -> handled by producer.poll()
        });

        this.producer.on(CONNECT, () => {
            logger.info(`KafkaJS producer is ready.`);
            super.emit("ready");
        });

        logger.debug("Connecting..");

        try {

            await Promise.all([
                this.producer.connect(),
                this._adminClient.connect(),
            ]);

        } catch (error) {
            super.emit("error", error);
            throw error;
        }
    }

    /**
     * returns a partition for a key
     * @private
     * @param {string} - message key
     * @param {number} - partition count of topic, if 0 defaultPartitionCount is used
     * @returns {string} - deterministic partition value for key
     */
    _getPartitionForKey (key, partitionCount = 1) {

        if (typeof key !== "string") {
            throw new Error("key must be a string.");
        }

        if (typeof partitionCount !== "number") {
            throw new Error("partitionCount must be number.");
        }

        return this._murmur(key, partitionCount);
    }

    /**
     * @async
     * produces a kafka message to a certain topic
     * @param {string} topicName - name of the topic to produce to
     * @param {object|string|null} message - value object for the message
     * @param {number} _partition - optional partition to produce to
     * @param {string} _key - optional message key
     * @param {string} _partitionKey - optional key to evaluate partition for this message
     * @returns {Promise.<object>}
     */
    async send (topicName, message, _partition = null, _key = null, _partitionKey = null) {

        /*
          these are not supported in the HighLevelProducer of node-rdkafka
          _opaqueKey = null,
          _headers = null,
        */

        if (!this.producer) {
            throw new Error("You must call and await .connect() before trying to produce messages.");
        }

        if (this.paused) {
            throw new Error("producer is paused.");
        }

        if (typeof message === "undefined" || !(typeof message === "string" || Buffer.isBuffer(message) || message === null)) {
            throw new Error("message must be a string, an instance of Buffer or null.");
        }

        const key = _key ? _key : uuidv4();
        if (message !== null) {
            message = Buffer.isBuffer(message) ? message : Buffer.from(message);
        }

        let maxPartitions: any = 0;
        //find correct max partition count
        if (this.defaultPartitionCount === "auto" && typeof _partition !== "number") { //manual check to improve performance
            maxPartitions = await this.getPartitionCountOfTopic(topicName);
            if (maxPartitions === -1) {
                throw new Error("defaultPartition set to 'auto', but was not able to resolve partition count for topic" +
                    topicName + ", please make sure the topic exists before starting the producer in auto mode.");
            }
        } else {
            maxPartitions = this.defaultPartitionCount;
        }

        let partition = 0;
        //find correct partition for this key
        if (maxPartitions >= 2 && typeof _partition !== "number") { //manual check to improve performance
            partition = this._getPartitionForKey(_partitionKey ? _partitionKey : key, maxPartitions);
        }

        //if _partition (manual) is set, it always overwrites a selected partition
        partition = typeof _partition === "number" ? _partition : partition;

        this.config.logger.debug(JSON.stringify({
            topicName,
            partition,
            key
        }));

        const producedAt = Date.now();

        this._lastProcessed = producedAt;
        this._totalSentMessages++;
        const acks = this.config
            && this.config.tconf &&
            this.config.tconf["request.required.acks"] || 1;

        await this.producer.send({
            topic: topicName,
            acks,
            messages: [
                { key: key, value: message, partition, timestamp: producedAt },
            ],
        });
    }

    /**
     * @async
     * produces a formatted message to a topic
     * @param {string} topic - topic to produce to
     * @param {string} identifier - identifier of message (is the key)
     * @param {object} payload - object (part of message value)
     * @param {number} partition - optional partition to produce to
     * @param {number} version - optional version of the message value
     * @param {string} partitionKey - optional key to evaluate partition for this message
     * @returns {Promise.<object>}
     */
    async buffer (topic, identifier, payload, partition = null, version = null, partitionKey = null) {

        if (typeof identifier === "undefined") {
            identifier = uuidv4();
        }

        if (typeof identifier !== "string") {
            identifier = identifier + "";
        }

        if (typeof payload !== "object") {
            throw new Error("expecting payload to be of type object.");
        }

        if (typeof payload.id === "undefined") {
            payload.id = identifier;
        }

        if (version && typeof payload.version === "undefined") {
            payload.version = version;
        }

        return await this.send(topic, JSON.stringify(payload), partition, identifier, partitionKey);
    }

    /**
     * @async
     * @private
     * produces a specially formatted message to a topic
     * @param {string} topic - topic to produce to
     * @param {string} identifier - identifier of message (is the key)
     * @param {object} _payload - object message value payload
     * @param {number} version - optional version (default is 1)
     * @param {*} _ -ignoreable, here for api compatibility
     * @param {string} partitionKey - optional key to deterministcally detect partition
     * @param {number} partition - optional partition (overwrites partitionKey)
     * @param {string} messageType - optional messageType (for the formatted message value)
     * @returns {Promise.<object>}
     */
    async _sendBufferFormat (topic, identifier, _payload, version = 1, _, partitionKey = null, partition = null, messageType = "") {

        if (typeof identifier === "undefined") {
            identifier = uuidv4();
        }

        if (typeof identifier !== "string") {
            identifier = identifier + "";
        }

        if (typeof _payload !== "object") {
            throw new Error("expecting payload to be of type object.");
        }

        if (typeof _payload.id === "undefined") {
            _payload.id = identifier;
        }

        if (version && typeof _payload.version === "undefined") {
            _payload.version = version;
        }

        const payload = {
            payload: _payload,
            key: identifier,
            id: uuidv4(),
            time: (new Date()).toISOString(),
            type: topic + messageType
        };

        return await this.send(topic, JSON.stringify(payload), partition, identifier, partitionKey);
    }

    /**
     * an alias for bufferFormatPublish()
     * @alias bufferFormatPublish
     */
    bufferFormat (topic, identifier, payload, version = 1, compressionType = 0, partitionKey = null) {
        return this.bufferFormatPublish(topic, identifier, payload, version, compressionType, partitionKey);
    }

    /**
     * produces a specially formatted message to a topic, with type "publish"
     * @param {string} topic - topic to produce to
     * @param {string} identifier - identifier of message (is the key)
     * @param {object} _payload - object message value payload
     * @param {number} version - optional version (default is 1)
     * @param {*} _ -ignoreable, here for api compatibility
     * @param {string} partitionKey - optional key to deterministcally detect partition
     * @param {number} partition - optional partition (overwrites partitionKey)
     * @returns {Promise.<object>}
     */
    bufferFormatPublish (topic, identifier, _payload, version = 1, _, partitionKey = null, partition = null) {
        return this._sendBufferFormat(topic, identifier, _payload, version, _, partitionKey, partition, MESSAGE_TYPES.PUBLISH);
    }

    /**
     * produces a specially formatted message to a topic, with type "update"
     * @param {string} topic - topic to produce to
     * @param {string} identifier - identifier of message (is the key)
     * @param {object} _payload - object message value payload
     * @param {number} version - optional version (default is 1)
     * @param {*} _ -ignoreable, here for api compatibility
     * @param {string} partitionKey - optional key to deterministcally detect partition
     * @param {number} partition - optional partition (overwrites partitionKey)
     * @returns {Promise.<object>}
     */
    bufferFormatUpdate (topic, identifier, _payload, version = 1, _, partitionKey = null, partition = null) {
        return this._sendBufferFormat(topic, identifier, _payload, version, _, partitionKey, partition, MESSAGE_TYPES.UPDATE);
    }

    /**
     * produces a specially formatted message to a topic, with type "unpublish"
     * @param {string} topic - topic to produce to
     * @param {string} identifier - identifier of message (is the key)
     * @param {object} _payload - object message value payload
     * @param {number} version - optional version (default is 1)
     * @param {*} _ -ignoreable, here for api compatibility
     * @param {string} partitionKey - optional key to deterministcally detect partition
     * @param {number} partition - optional partition (overwrites partitionKey)
     * @returns {Promise.<object>}
     */
    bufferFormatUnpublish (topic, identifier, _payload, version = 1, _, partitionKey = null, partition = null) {
        return this._sendBufferFormat(topic, identifier, _payload, version, _, partitionKey, partition, MESSAGE_TYPES.UNPUBLISH);
    }

    /**
     * produces a tombstone (null payload with -1 size) message
     * on a key compacted topic/partition this will delete all occurances of the key
     * @param {string} topic - name of the topic
     * @param {string} key - key
     * @param {number|null} _partition - optional partition
     */
    tombstone (topic, key, _partition = null) {

        if (!key) {
            return Promise.reject(new Error("Tombstone messages only work on a key compacted topic, please provide a key."));
        }

        return this.send(topic, null, _partition, key, null);
    }

    /**
     * pauses production (sends will not be queued)
     */
    pause () {
        this.paused = true;
    }

    /**
     * resumes production
     */
    resume () {
        this.paused = false;
    }

    /**
     * returns producer statistics
     * @returns {object}
     */
    getStats () {
        return {
            totalPublished: this._totalSentMessages,
            last: this._lastProcessed,
            isPaused: this.paused,
            totalErrors: this._errors
        };
    }

    /**
     * @deprecated
     */
    refreshMetadata () {
        throw new Error("refreshMetadata not implemented for nproducer.");
    }

    /**
     * resolve the metadata information for a give topic
     * will create topic if it doesnt exist
     * @param {string} topic - name of the topic to query metadata for
     * @param {number} timeout - optional, default is 2500
     * @returns {Promise.<Metadata>}
     */
    getTopicMetadata (topic, timeout = 2500) {
        return new Promise((resolve, reject) => {

            if (!this.producer) {
                return reject(new Error("You must call and await .connect() before trying to get metadata."));
            }

            this._adminClient.fetchTopicMetadata({
                topics: [topic],
                timeout
            }, (error, raw) => {

                if (error) {
                    return reject(error);
                }

                resolve(new Metadata(raw[0]));
            });
        });
    }

    /**
     * @alias getTopicMetadata
     * @param {number} timeout - optional, default is 2500
     * @returns {Promise.<Metadata>}
     */
    getMetadata (timeout = 2500) {
        return this.getTopicMetadata(null, timeout);
    }

    /**
     * returns a list of available kafka topics on the connected brokers
     * @param {number} timeout
     */
    async getTopicList (timeout = 2500) {
        const metadata: any = await this.getMetadata(timeout);
        return metadata.asTopicList();
    }

    /**
     * @async
     * gets the partition count of the topic from the brokers metadata
     * keeps a local cache to speed up future requests
     * resolves to -1 if an error occures
     * @param {string} topic - name of topic
     * @returns {Promise.<number>}
     */
    async getPartitionCountOfTopic (topic) {

        if (!this.producer) {
            throw new Error("You must call and await .connect() before trying to get metadata.");
        }

        //prevent long running leaks..
        if (Object.keys(this._partitionCounts).length > MAX_PART_STORE_SIZE) {
            this._partitionCounts = {};
        }

        const now = Date.now();
        if (!this._partitionCounts[topic] || this._partitionCounts[topic].requested + MAX_PART_AGE_MS < now) {

            let count = -1;
            try {
                const metadata: any = await this.getMetadata(); //prevent creation of topic, if it does not exist
                count = metadata.getPartitionCountOfTopic(topic);
            } catch (error) {
                super.emit("error", new Error(`Failed to get metadata for topic ${topic}, because: ${error.message}.`));
                return -1;
            }

            this._partitionCounts[topic] = {
                requested: now,
                count
            };

            return count;
        }

        return this._partitionCounts[topic].count;
    }

    /**
     * gets the local partition count cache
     * @returns {object}
     */
    getStoredPartitionCounts () {
        return this._partitionCounts;
    }

    /**
     * @private
     * resets internal values
     */
    _reset () {
        this._lastProcessed = null;
        this._totalSentMessages = 0;
        this.paused = false;
        this._inClosing = false;
        this._partitionCounts = {};
        this._analytics = null;
        this._errors = 0;
    }

    /**
     * closes connection if open
     * stops poll interval if open
     */
    async close () {

        this.haltAnalytics();

        if (this.producer) {
            this._inClosing = true;
            clearInterval(this._producerPollIntv);

            try {
                await Promise.all([
                    this.producer.disconnect(),
                    this._adminClient.disconnect(),
                ]);
            } catch (error) {
                // Do nothing, silently closing
            }

            //this.producer = null;
        }
    }

    /**
     * called in interval
     * @private
     */
    _runAnalytics () {

        if (!this._analytics) {
            this._analytics = new ProducerAnalytics(this, this._analyticsOptions || {}, this.config.logger);
        }

        this._analytics.run()
            .then(res => super.emit("analytics", res))
            .catch(error => super.emit("error", error));
    }

    /**
     * returns the last computed analytics results
     * @throws
     * @returns {object}
     */
    getAnalytics () {

        if (!this._analytics) {
            super.emit("error", new Error("You have not enabled analytics on this consumer instance."));
            return {};
        }

        return this._analytics.getLastResult();
    }

    /**
     * runs a health check and returns object with status and message
     * @returns {Promise.<object>}
     */
    checkHealth () {
        return this._health.check();
    }
}

