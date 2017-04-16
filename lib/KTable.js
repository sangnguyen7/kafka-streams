"use strict";

const EventEmitter = require("events");
const most = require("most");
const {create} = require("@most/create");
const Promise = require("bluebird");

const StreamDSL = require("./StreamDSL.js");
const {LastState} = require("./actions");
const KStream = require("./KStream.js");

const MESSAGE = "message";
const NOOP = () => {};

class KTable extends StreamDSL {

    /**
     * gives a table representation of a stream
     * keyMapETL = v -> {key, value} (sync)
     * @param topicName
     * @param keyMapETL
     * @param storage
     * @param kafka
     */
    constructor(topicName, keyMapETL, storage = null, kafka = null){
        super(topicName, storage, kafka);

        //KTable only works on {key, value} payloads
        if(typeof keyMapETL !== "function"){
            throw new Error("keyMapETL must be a valid function.");
        }

        this._tee = new EventEmitter();
        this.started = false;
        this.finalised = false;

        //will close on first stream$.onComplete()
        this.consumerOpen = true;

        this.map(keyMapETL);
    }

    /**
     * start kafka consumption
     * prepare production of messages if necessary
     * when called with zero or just a single callback argument
     * this function will return a promise and use the callback for errors
     * @param kafkaReadyCallback
     * @param kafkaErrorCallback
     */
    start(kafkaReadyCallback = null, kafkaErrorCallback = null){

        if(arguments.length < 2){
            return new Promise(resolve => {
                this._start(resolve, kafkaErrorCallback);
            });
        }

        return this._start(kafkaReadyCallback, kafkaErrorCallback);
    }

    _start(kafkaReadyCallback = null, kafkaErrorCallback = null){

        if(this.started){
            throw new Error("this KTable is already started.");
        }

        this.started = true;

        if(!this.topicName && !this.produceAsTopic){
            return kafkaReadyCallback(); //possibly a good idea to skip finalise()
        }

        if(!this.finalised){
            this.finalise();
        }

        let producerReady = false;
        let consumerReady = false;

        const onReady = (type) => {
            switch(type){
                case "producer": producerReady = true; break;
                case "consumer": consumerReady = true; break;
            }

            //consumer && producer
            if(producerReady && consumerReady && kafkaReadyCallback){
                kafkaReadyCallback();
            }

            //consumer only
            if(!this.produceAsTopic && consumerReady && kafkaReadyCallback){
                kafkaReadyCallback();
            }

            //producer only
            if(this.produceAsTopic && producerReady && kafkaReadyCallback && !this.kafka.topic){
                kafkaReadyCallback();
            }
        };

        this.kafka.on(MESSAGE, msg => {

            if(!this.consumerOpen){
                return;
            }

            super.writeToStream(msg);
        });

        this.kafka.start(() => { onReady("consumer"); }, kafkaErrorCallback, this.produceAsTopic);

        if(this.produceAsTopic){
            this.kafka.setupProducer(this.outputTopicName, this.outputPartitionsCount, () => { onReady("producer"); },
                kafkaErrorCallback);

            super.forEach(message => {
                this.kafka.send(this.outputTopicName, message).catch(e => {
                    if(kafkaErrorCallback){
                        kafkaErrorCallback(e);
                    }
                });
            });
        }
    }

    /**
     * Emits an output when both input sources have records with the same key.
     * @param stream
     * @param key
     */
    innerJoin(stream, key = "key"){
        //TODO
    }

    /**
     * Emits an output for each record in either input source.
     * If only one source contains a key, the other is null
     * @param stream
     */
    outerJoin(stream){
        //TODO
    }

    /**
     * Emits an output for each record in the left or primary input source.
     * If the other source does not have a value for a given key, it is set to null
     * @param stream
     */
    leftJoin(stream){
        //TODO
    }

    merge(){
        //TODO
    }

    writeToTableStream(message){
        this._tee.emit(MESSAGE, message);
    }

    finalise(){

        if(this.finalised){
            throw new Error("this KTable has already been finalised.");
        }

        // a KStream is a simple changelog implementation (which StreamDSL delivers by default)
        // a KTable is a table stream representation meaning that only the latest representation of
        // a message must be present

        const lastState = new LastState(this.storage);
        this.asyncMap(lastState.execute.bind(lastState));

        this.stream$.forEach(NOOP).then(_ => {
            //streams until completed
            //TODO maybe close kafka-consumer here?
            this.consumerOpen = false;
        });

        this.stream$ = most.merge(this.stream$, most.fromEvent(MESSAGE, this._tee));
        this.finalised = true;
    }

    consumeUntilMs(ms = 1000){

        const signal = create((add, end, _) => {
            setTimeout(() => {
                add("signal");
                end();
            }, ms);
            return () => {};
        });

        this.stream$ = this.stream$.until(signal);

        if(!this.finalised){
            this.finalise();
        }

        return this;
    }

    consumeUntilCount(count = 1000){

        this.stream$ = this.stream$.take(count);

        if(!this.finalised){
            this.finalise();
        }

        return this;
    }

    consumeUntilLatestOffset(){

        //TODO

        if(!this.finalised){
            this.finalise();
        }
    }

    getTable(){
        return this.storage.getState();
    }

    replay(){
        Object.keys(this.storage.state).forEach(key => {

            const message = {
                key: key,
                value: this.storage.state[key]
            };

            this.writeToTableStream(message);
        });
    }

    close(){
        this.kafka.close();
    }
}

module.exports = KTable;