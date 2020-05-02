"use strict";
const uuid = require("uuid");
const { Kafka, PartitionDrainer, Drainer, Publisher } = require("sinek");
const debug = require("debug")("kafka-streams:jsclient");
const KafkaClient = require("./KafkaClient.js");
const NOOP = () => { };
class JSKafkaClient extends KafkaClient {
    constructor(topic, config) {
        super();
        this.topic = topic;
        this.config = config;
        this.kafkaConsumerClient = null;
        this.kafkaProducerClient = null;
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
    start(readyCallback = null, kafkaErrorCallback = null, withProducer = false, withBackPressure = false) {
        if (!this.topic || !this.topic.length) {
            return;
        }
        const { zkConStr, kafkaHost, logger, groupId, workerPerPartition, options } = this.config;
        let conStr = null;
        if (typeof kafkaHost === "string") {
            conStr = kafkaHost;
        }
        if (typeof zkConStr === "string") {
            conStr = zkConStr;
        }
        if (conStr === null) {
            throw new Error("One of the following: zkConStr or kafkaHost must be defined.");
        }
        this.kafkaConsumerClient = new Kafka(conStr, logger, conStr === kafkaHost);
        this.kafkaConsumerClient.on("ready", () => {
            debug("consumer ready");
            if (readyCallback) {
                readyCallback();
            }
        });
        this.kafkaConsumerClient.on("error", kafkaErrorCallback || NOOP);
        this.kafkaConsumerClient.becomeConsumer(this.topic, groupId, options || {});
        if (withBackPressure) {
            this.consumer = new PartitionDrainer(this.kafkaConsumerClient, workerPerPartition || 1, false, false);
        }
        else {
            this.consumer = new Drainer(this.kafkaConsumerClient, workerPerPartition, false, true);
        }
        super.once("kafka-producer-ready", () => {
            if (withBackPressure) {
                if (this.topic.length > 1) {
                    throw new Error("JS Client does not support multiple topics in backpressure mode.");
                }
                this.consumer.drain(this.topic[0], (message, done) => {
                    super.emit("message", message);
                    done();
                }).catch(e => kafkaErrorCallback(e));
            }
            else {
                this.consumer.drain((message, done) => {
                    super.emit("message", message);
                    done();
                });
            }
        });
        if (!withProducer) {
            super.emit("kafka-producer-ready", true);
        }
    }
    setupProducer(produceTopic, partitions = 1, readyCallback = null, kafkaErrorCallback = null, outputKafkaConfig = null) {
        this.produceTopic = produceTopic || this.produceTopic;
        this.producePartitionCount = partitions;
        const { zkConStr, kafkaHost, logger, clientName, options } = (outputKafkaConfig || this.config);
        let conStr = null;
        if (typeof kafkaHost === "string") {
            conStr = kafkaHost;
        }
        if (typeof zkConStr === "string") {
            conStr = zkConStr;
        }
        if (conStr === null) {
            throw new Error("One of the following: zkConStr or kafkaHost must be defined.");
        }
        if (!this.kafkaProducerClient) {
            this.kafkaProducerClient = new Kafka(conStr, logger, conStr === kafkaHost);
            this.kafkaProducerClient.on("ready", () => {
                debug("producer ready");
                super.emit("kafka-producer-ready", true);
                if (readyCallback) {
                    readyCallback();
                }
            });
            this.kafkaProducerClient.on("error", kafkaErrorCallback || NOOP);
        }
        this.kafkaProducerClient.becomeProducer([this.produceTopic], clientName, options);
        this.producer = new Publisher(this.kafkaProducerClient, partitions || 1);
    }
    send(topic, message) {
        if (!this.producer) {
            return Promise.reject("producer is not yet setup.");
        }
        let partition = -1;
        if (this.producePartitionCount < 2) {
            partition = 0;
        }
        else {
            partition = KafkaClient._getRandomIntInclusive(0, this.producePartitionCount);
        }
        return this.producer.send(topic, Array.isArray(message) ? message : [message], null, partition, 0);
    }
    buffer(topic, identifier, payload, compressionType = 0) {
        if (!this.producer) {
            return Promise.reject("producer is not yet setup.");
        }
        if (!identifier) {
            identifier = uuid.v4();
        }
        return this.producer.appendBuffer(topic, identifier, payload, compressionType);
    }
    bufferFormat(topic, identifier, payload, version = 1, compressionType = 0) {
        if (!this.producer) {
            return Promise.reject("producer is not yet setup.");
        }
        if (!identifier) {
            identifier = uuid.v4();
        }
        return this.producer.bufferPublishMessage(topic, identifier, payload, version, compressionType);
    }
    pause() {
        if (this.consumer) {
            this.consumer.pause();
        }
        if (this.producer) {
            this.producer.pause();
        }
    }
    resume() {
        if (this.consumer) {
            this.consumer.resume();
        }
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
            this.consumer = null;
        }
        if (this.producer) {
            this.producer.close();
            this.producer = null;
        }
    }
    closeConsumer() {
        if (this.consumer) {
            this.consumer.close();
            this.consumer = null;
        }
    }
}
module.exports = JSKafkaClient;
//# sourceMappingURL=JSKafkaClient.js.map