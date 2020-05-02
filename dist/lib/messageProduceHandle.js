"use strict";
const debug = require("debug")("kafka-streams:mph");
const PRODUCE_TYPES = require("./produceTypes.js");
const hasKVStructure = message => {
    if (message &&
        typeof message === "object" &&
        typeof message.key !== "undefined" &&
        typeof message.value !== "undefined") {
        return true;
    }
    return false;
};
const produceTypeSelection = (produceType, kafka, compressionType, topic, partition, key, value, partitionKey = null, opaqueKey = null, version = 1) => {
    debug("producing", produceType, topic, partition, key, partitionKey, opaqueKey, version, value);
    switch (produceType) {
        case PRODUCE_TYPES.SEND:
            return kafka.send(topic, value, partition, key, partitionKey, opaqueKey);
        case PRODUCE_TYPES.BUFFER:
            return kafka.buffer(topic, key, value, compressionType, partition, version, partitionKey);
        case PRODUCE_TYPES.BUFFER_FORMAT:
            return kafka.bufferFormat(topic, key, value, version, compressionType, partitionKey, partition);
        default:
            return Promise.reject(new Error(`${produceType} is an unknown produceType.`));
    }
};
const messageProduceHandle = (kafka, message, outputTopicName, produceType, compressionType, version, producerErrorCallback) => {
    let _topic = outputTopicName;
    let _key = null;
    let _version = version;
    let _partition = null;
    let _partitionKey = null;
    let _opaqueKey = null;
    let _value = message;
    if (hasKVStructure(message)) {
        _key = message.key;
        if (message.topic) {
            _topic = message.topic;
        }
        if (typeof message.version !== "undefined") {
            _version = message.version;
        }
        if (typeof message.partition !== "undefined") {
            _partition = message.partition;
        }
        if (typeof message.partitionKey !== "undefined") {
            _partitionKey = message.partitionKey;
        }
        if (typeof message.opaqueKey !== "undefined") {
            _opaqueKey = message.opaqueKey;
        }
        _value = message.value;
    }
    const kafkaMessage = {
        produceType,
        compressionType,
        topic: _topic,
        partition: _partition,
        key: _key,
        value: _value,
        partitionKey: _partitionKey,
        opaqueKey: _opaqueKey
    };
    const produceHandler = kafka.getProduceHandler();
    if (produceHandler) {
        produceHandler.emit("produced", kafkaMessage);
    }
    return produceTypeSelection(produceType, kafka, compressionType, _topic, _partition, _key, _value, _partitionKey, _opaqueKey, _version).then((produceMessageValue) => {
        debug("Produce successfull", kafkaMessage.topic, produceMessageValue);
        if (produceHandler) {
            produceHandler.emit("delivered", kafkaMessage, produceMessageValue);
        }
    }).catch((error) => {
        debug("Produce failed", kafkaMessage, error.message);
        if (producerErrorCallback) {
            error.message = "During message produce: " + error.message;
            producerErrorCallback(error);
        }
    });
};
module.exports = {
    messageProduceHandle
};
//# sourceMappingURL=messageProduceHandle.js.map