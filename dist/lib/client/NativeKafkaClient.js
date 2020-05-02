"use strict";
const uuid = require("uuid");
const { NConsumer, NProducer } = require("sinek");
const debug = require("debug")("kafka-streams:nativeclient");
const KafkaClient = require("./KafkaClient.js");
const NOOP = () => { };
class NativeKafkaClient extends KafkaClient {
    constructor(topic, config, batchOptions = undefined) {
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
    setProduceHandler(handler) {
        this._produceHandler = handler;
    }
    getProduceHandler() {
        return this._produceHandler;
    }
    overwriteTopics(topics) {
        this.topic = topics;
    }
    adjustDefaultPartitionCount(partitionCount = 1) {
        this.producePartitionCount = partitionCount;
        this.producer.defaultPartitionCount = partitionCount;
    }
    start(readyCallback = null, kafkaErrorCallback = null, withProducer = false, withBackPressure = false) {
        if (!this.topic || !this.topic.length) {
            return;
        }
        if (this.batchOptions) {
            withBackPressure = true;
        }
        kafkaErrorCallback = kafkaErrorCallback || NOOP;
        this.consumer = new NConsumer(this.topic, this.config);
        this.consumer.on("ready", readyCallback || NOOP);
        this.consumer.on("error", kafkaErrorCallback);
        super.once("kafka-producer-ready", () => {
            const streamOptions = {
                asString: false,
                asJSON: false
            };
            this.consumer.connect(!withBackPressure, streamOptions).then(() => {
                debug("consumer ready");
                if (withBackPressure) {
                    return this.consumer.consume((message, done) => {
                        super.emit("message", message);
                        done();
                    }, false, false, this.batchOptions);
                }
                else {
                    this.consumer.on("message", message => {
                        super.emit("message", message);
                    });
                    return this.consumer.consume();
                }
            }).catch(e => kafkaErrorCallback(e));
        });
        if (!withProducer) {
            super.emit("kafka-producer-ready", true);
        }
    }
    setupProducer(produceTopic, partitions = 1, readyCallback = null, kafkaErrorCallback = null, outputKafkaConfig = null) {
        this.produceTopic = produceTopic || this.produceTopic;
        this.producePartitionCount = partitions;
        kafkaErrorCallback = kafkaErrorCallback || NOOP;
        const config = outputKafkaConfig || this.config;
        if (!this.producer) {
            this.producer = new NProducer(config, [this.produceTopic], this.producePartitionCount);
            this.producer.on("ready", () => {
                debug("producer ready");
                super.emit("kafka-producer-ready", true);
                if (readyCallback) {
                    readyCallback();
                }
            });
            this.producer.on("error", kafkaErrorCallback);
            this.producer.connect().catch(e => kafkaErrorCallback(e));
        }
    }
    send(topicName, message, partition = null, key = null, partitionKey = null, opaqueKey = null) {
        if (!this.producer) {
            return Promise.reject("producer is not yet setup.");
        }
        return this.producer.send(topicName, message, partition, key, partitionKey, opaqueKey);
    }
    buffer(topic, identifier, payload, _ = null, partition = null, version = null, partitionKey = null) {
        if (!this.producer) {
            return Promise.reject("producer is not yet setup.");
        }
        return this.producer.buffer(topic, identifier, payload, partition, version, partitionKey);
    }
    bufferFormat(topic, identifier, payload, version = 1, _ = null, partitionKey = null, partition = null) {
        if (!this.producer) {
            return Promise.reject("producer is not yet setup.");
        }
        if (!identifier) {
            identifier = uuid.v4();
        }
        return this.producer.bufferFormatPublish(topic, identifier, payload, version, undefined, partitionKey, partition);
    }
    pause() {
        if (this.producer) {
            this.producer.pause();
        }
    }
    resume() {
        if (this.producer) {
            this.producer.resume();
        }
    }
    getStats() {
        return {
            inTopic: this.topic ? this.topic : null,
            consumer: this.consumer ? this.consumer.getStats() : null,
            outTopic: this.produceTopic ? this.produceTopic : null,
            producer: this.producer ? this.producer.getStats() : null
        };
    }
    close(commit = false) {
        if (this.consumer) {
            this.consumer.close(commit);
        }
        if (this.producer) {
            this.producer.close();
        }
    }
    closeConsumer(commit = false) {
        if (this.consumer) {
            this.consumer.close(commit);
            this.consumer = null;
        }
    }
}
module.exports = NativeKafkaClient;
//# sourceMappingURL=NativeKafkaClient.js.map