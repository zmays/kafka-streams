"use strict";
const EventEmitter = require("events");
const most = require("most");
const Promise = require("bluebird");
const uuid = require("uuid");
const debug = require("debug")("kafka-streams:streamdsl");
const KStorage = require("../KStorage.js");
const KafkaClient = require("../client/KafkaClient.js");
const { KeyCount, Sum, Max, Min } = require("../actions/index.js");
const { messageProduceHandle } = require("../messageProduceHandle.js");
const PRODUCE_TYPES = require("../produceTypes.js");
const NOOP = () => { };
const MESSAGE = "message";
const DEFAULT_AUTO_FLUSH_BUFFER_SIZE = 100;
class StreamDSL {
    constructor(topicName, storage = null, kafka = null, isClone = false) {
        debug("stream-dsl from clone", isClone, "for", topicName);
        if (!isClone && (!kafka || !(kafka instanceof KafkaClient))) {
            throw new Error("kafka has to be an instance of KafkaClient.");
        }
        if (!storage || !(storage instanceof KStorage)) {
            throw new Error("storage hsa to be an instance of KStorage.");
        }
        if (!topicName) {
            this.noTopicProvided = true;
        }
        else {
            this.noTopicProvided = false;
            if (!Array.isArray(topicName)) {
                topicName = [topicName];
            }
        }
        this.topicName = topicName || [];
        this.kafka = kafka;
        this.storage = storage;
        this.isClone = isClone;
        if (!(this.storage instanceof KStorage)) {
            throw new Error("storage must be an instance of KStorage.");
        }
        this._ee = new EventEmitter();
        this.stream$ = most.fromEvent(MESSAGE, this._ee);
        this.produceAsTopic = false;
        this.outputTopicName = null;
        this.outputPartitionsCount = 1;
        this.produceType = PRODUCE_TYPES.SEND;
        this.produceVersion = 1;
        this.produceCompressionType = 0;
        this._kafkaStreams = null;
        this.PRODUCE_TYPES = PRODUCE_TYPES;
        this.DEFAULT_AUTO_FLUSH_BUFFER_SIZE = DEFAULT_AUTO_FLUSH_BUFFER_SIZE;
    }
    start() {
        return Promise.reject("When inherting StreamDSL, the start method should be overwritten with connector logic.");
    }
    getStats() {
        return this.kafka ? this.kafka.getStats() : null;
    }
    getStorage() {
        return this.storage;
    }
    writeToStream(message) {
        if (!Array.isArray(message)) {
            return this._ee.emit("message", message);
        }
        message.forEach(_message => {
            this._ee.emit("message", _message);
        });
    }
    getMost() {
        return this.stream$;
    }
    getNewMostFrom(array = []) {
        return most.from(array);
    }
    replaceInternalObservable(newStream$) {
        this._ee.removeAllListeners(MESSAGE);
        this._ee = new EventEmitter();
        this.stream$ = most.merge(newStream$, most.fromEvent(MESSAGE, this._ee));
    }
    setProduceHandler(handler) {
        if (!handler || !(handler instanceof EventEmitter)) {
            throw new Error("ProduceHandler must be an instance of EventEmitter (events).");
        }
        this.kafka.setProduceHandler(handler);
    }
    createAndSetProduceHandler() {
        const ee = new EventEmitter();
        this.setProduceHandler(ee);
        return ee;
    }
    setKafkaStreamsReference(reference) {
        this._kafkaStreams = reference;
    }
    from(topicName) {
        if (!Array.isArray(topicName)) {
            topicName = [topicName];
        }
        topicName.forEach(topic => {
            this.topicName.push(topic);
        });
        if (this.noTopicProvided) {
            this.noTopicProvided = false;
        }
        return this;
    }
    awaitPromises(etl) {
        this.stream$ = this.stream$.awaitPromises(etl);
        return this;
    }
    map(etl) {
        this.stream$ = this.stream$.map(etl);
        return this;
    }
    asyncMap(etl) {
        this.stream$ = this.stream$.flatMap(value => most.fromPromise(etl(value)));
        return this;
    }
    concatMap(etl) {
        this.stream$ = this.stream$.concatMap(etl);
        return this;
    }
    forEach(eff) {
        return this.stream$.forEach(eff);
    }
    chainForEach(eff, callback = null) {
        this.stream$ = this.stream$.multicast();
        this.stream$.forEach(eff).then(r => {
            if (callback) {
                callback(null, r);
            }
        }, e => {
            if (callback) {
                callback(e);
            }
        });
        return this;
    }
    tap(eff) {
        this.stream$ = this.stream$.tap(eff);
        return this;
    }
    filter(pred) {
        this.stream$ = this.stream$.filter(pred);
        return this;
    }
    skipRepeats() {
        this.stream$ = this.stream$.skipRepeats();
        return this;
    }
    skipRepeatsWith(equals) {
        this.stream$ = this.stream$.skipRepeatsWith(equals);
        return this;
    }
    skip(count) {
        this.stream$ = this.stream$.skip(count);
        return this;
    }
    take(count) {
        this.stream$ = this.stream$.take(count);
        return this;
    }
    multicast() {
        this.stream$ = this.stream$.multicast();
        return this;
    }
    mapStringToArray(delimiter = " ") {
        return this.map(element => {
            if (!element || typeof element !== "string") {
                return element;
            }
            return element.split(delimiter);
        });
    }
    mapArrayToKV(keyIndex = 0, valueIndex = 1) {
        return this.map(element => {
            if (!Array.isArray(element)) {
                return element;
            }
            return {
                key: element[keyIndex],
                value: element[valueIndex]
            };
        });
    }
    mapStringToKV(delimiter = " ", keyIndex = 0, valueIndex = 1) {
        this.mapStringToArray(delimiter);
        this.mapArrayToKV(keyIndex, valueIndex);
        return this;
    }
    mapJSONParse() {
        return this.map(string => {
            if (typeof string !== "string") {
                return string;
            }
            try {
                return JSON.parse(string);
            }
            catch (e) {
                return e;
            }
        });
    }
    mapStringify() {
        return this.map(object => {
            if (typeof object !== "object") {
                return object;
            }
            return JSON.stringify(object);
        });
    }
    mapBufferKeyToString() {
        return this.map(object => {
            if (typeof object !== "object" || !object.key) {
                return object;
            }
            if (Buffer.isBuffer(object.key)) {
                return object;
            }
            try {
                const key = object.key.toString("utf8");
                if (key) {
                    object.key = key;
                }
            }
            catch (_) {
            }
            return object;
        });
    }
    mapBufferValueToString() {
        return this.map(object => {
            if (typeof object !== "object" || !object.value) {
                return object;
            }
            if (typeof object.value === "string") {
                return object;
            }
            try {
                const value = object.value.toString("utf8");
                if (value) {
                    object.value = value;
                }
            }
            catch (_) {
            }
            return object;
        });
    }
    mapStringValueToJSONObject() {
        return this.map(object => {
            if (typeof object !== "object" || !object.value) {
                return object;
            }
            if (typeof object.value === "object") {
                return object;
            }
            try {
                const value = JSON.parse(object.value);
                if (value) {
                    object.value = value;
                }
            }
            catch (_) {
            }
            return object;
        });
    }
    mapJSONConvenience() {
        return this
            .mapBufferKeyToString()
            .mapBufferValueToString()
            .mapStringValueToJSONObject();
    }
    wrapAsKafkaValue(topic = undefined) {
        return this.map(any => {
            return {
                opaqueKey: null,
                partitionKey: null,
                partition: null,
                key: null,
                value: any,
                topic
            };
        });
    }
    mapWrapKafkaValue() {
        return this.map(message => {
            if (typeof message === "object" &&
                typeof message.value !== "undefined") {
                return message.value;
            }
            return message;
        });
    }
    atThroughput(count = 1, callback) {
        let countState = 0;
        this.tap(message => {
            if (countState > count) {
                return;
            }
            countState++;
            if (count === countState) {
                callback(message);
            }
        });
        return this;
    }
    mapToFormat(type = "unknown-publish", getId = null) {
        this.map(message => {
            const id = getId ? getId(message) : uuid.v4();
            return {
                payload: message,
                time: (new Date()).toISOString(),
                type,
                id
            };
        });
        return this;
    }
    mapFromFormat() {
        this.map(message => {
            if (typeof message === "object") {
                return message.payload;
            }
            try {
                const object = JSON.parse(message);
                if (typeof object === "object") {
                    return object.payload;
                }
            }
            catch (e) {
            }
            return message;
        });
        return this;
    }
    timestamp(etl) {
        if (!etl) {
            this.stream$ = this.stream$.timestamp();
            return this;
        }
        this.stream$ = this.stream$.map(element => {
            return {
                time: etl(element),
                value: element
            };
        });
        return this;
    }
    constant(substitute) {
        this.stream$ = this.stream$.constant(substitute);
        return this;
    }
    scan(eff, initial) {
        this.stream$ = this.stream$.scan(eff, initial);
        return this;
    }
    slice(start, end) {
        this.stream$ = this.stream$.slice(start, end);
        return this;
    }
    takeWhile(pred) {
        this.stream$ = this.stream$.takeWhile(pred);
        return this;
    }
    skipWhile(pred) {
        this.stream$ = this.stream$.skipWhile(pred);
        return this;
    }
    until(signal$) {
        this.stream$ = this.stream$.until(signal$);
        return this;
    }
    since(signal$) {
        this.stream$ = this.stream$.since(signal$);
        return this;
    }
    continueWith(f) {
        this.stream$ = this.stream$.continueWith(f);
        return this;
    }
    reduce(eff, initial) {
        return this.stream$.reduce(eff, initial);
    }
    chainReduce(eff, initial, callback) {
        this.stream$ = this.stream$.multicast();
        this.stream$.reduce(eff, initial).then(r => {
            if (callback) {
                callback(null, r);
            }
        }, e => {
            if (callback) {
                callback(e);
            }
        });
        return this;
    }
    drain() {
        return this.stream$.drain();
    }
    throttle(throttlePeriod) {
        this.stream$ = this.stream$.throttle(throttlePeriod);
        return this;
    }
    delay(delayTime) {
        this.stream$ = this.stream$.delay(delayTime);
        return this;
    }
    debounce(debounceTime) {
        this.stream$ = this.stream$.debounce(debounceTime);
        return this;
    }
    countByKey(key = "key", countFieldName = "count") {
        const keyCount = new KeyCount(this.storage, key, countFieldName);
        this.asyncMap(keyCount.execute.bind(keyCount));
        return this;
    }
    sumByKey(key = "key", fieldName = "value", sumField = false) {
        const sum = new Sum(this.storage, key, fieldName, sumField);
        this.asyncMap(sum.execute.bind(sum));
        return this;
    }
    min(fieldName = "value", minField = "min") {
        const min = new Min(this.storage, fieldName, minField);
        this.asyncMap(min.execute.bind(min));
        return this;
    }
    max(fieldName = "value", maxField = "max") {
        const max = new Max(this.storage, fieldName, maxField);
        this.asyncMap(max.execute.bind(max));
        return this;
    }
    _join() {
        this.stream$ = most.join(this.stream$);
        return this;
    }
    _merge(otherStream$) {
        this.stream$ = most.merge(this.stream$, otherStream$);
        return this;
    }
    _zip(otherStream$, combine) {
        this.stream$ = this.stream$.zip(combine, otherStream$);
        return this;
    }
    _combine(otherStream$, combine) {
        this.stream$ = this.stream$.combine(combine, otherStream$);
        return this;
    }
    _sample(sampleStream$, otherStream$, combine) {
        this.stream$ = sampleStream$.sample(combine, this.stream$, otherStream$);
        return this;
    }
    to(topic = undefined, outputPartitionsCount = 1, produceType = "send", version = 1, compressionType = 0, producerErrorCallback = null, outputKafkaConfig = null) {
        return new Promise((resolve, reject) => {
            if (this.produceAsTopic) {
                return reject(new Error(".to() has already been called on this dsl instance."));
            }
            this.produceAsTopic = true;
            if (topic && typeof topic === "object") {
                if (topic.outputPartitionsCount) {
                    outputPartitionsCount = topic.outputPartitionsCount;
                }
                if (topic.produceType) {
                    produceType = topic.produceType;
                }
                if (topic.version) {
                    version = topic.version;
                }
                if (topic.compressionType) {
                    compressionType = topic.compressionType;
                }
                if (topic.producerErrorCallback) {
                    producerErrorCallback = topic.producerErrorCallback;
                }
                if (topic.outputKafkaConfig) {
                    outputKafkaConfig = topic.outputKafkaConfig;
                }
                if (topic.topic) {
                    topic = topic.topic;
                }
            }
            produceType = produceType || "";
            produceType = produceType.toLowerCase();
            const produceTypes = Object.keys(PRODUCE_TYPES).map(k => PRODUCE_TYPES[k]);
            if (produceTypes.indexOf(produceType) === -1) {
                return reject(new Error(`produceType must be a supported types: ${produceTypes.join(", ")}.`));
            }
            this.outputTopicName = topic;
            this.outputPartitionsCount = outputPartitionsCount;
            this.produceType = produceType;
            this.produceVersion = version;
            this.produceCompressionType = compressionType;
            if (!this.isClone) {
                return resolve(true);
            }
            if (!this.kafka || !this.kafka.setupProducer) {
                return reject(new Error("setting .to() on a cloned KStream requires a kafka client to injected during merge."));
            }
            if (!this._kafkaStreams) {
                return reject(new Error("KafkaStreams reference missing on stream instance, failed to setup to(..)"));
            }
            const oldProducerErrorCallback = producerErrorCallback;
            producerErrorCallback = (error) => {
                if (oldProducerErrorCallback) {
                    oldProducerErrorCallback(error);
                }
                this._kafkaStreams.emit("error", error);
            };
            this.kafka.setupProducer(this.outputTopicName, this.outputPartitionsCount, resolve, producerErrorCallback || NOOP, outputKafkaConfig);
            this.forEach(message => {
                messageProduceHandle(this.kafka, message, this.outputTopicName, this.produceType, this.produceCompressionType, this.produceVersion, producerErrorCallback);
            });
        });
    }
}
module.exports = StreamDSL;
//# sourceMappingURL=StreamDSL.js.map