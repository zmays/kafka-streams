"use strict";
const EventEmitter = require("events");
const most = require("most");
const Promise = require("bluebird");
const StreamDSL = require("./StreamDSL.js");
const { LastState } = require("../actions/index");
const StorageMerger = require("../StorageMerger.js");
const { messageProduceHandle } = require("../messageProduceHandle.js");
const MESSAGE = "message";
const NOOP = () => { };
class KTable extends StreamDSL {
    constructor(topicName, keyMapETL, storage = null, kafka = null, isClone = false) {
        super(topicName, storage, kafka, isClone);
        if (typeof keyMapETL !== "function") {
            throw new Error("keyMapETL must be a valid function.");
        }
        this._tee = new EventEmitter();
        this.started = false;
        this.finalised = false;
        this.consumerOpen = true;
        if (!isClone) {
            this.map(keyMapETL);
        }
        else {
            this.consumerOpen = false;
            this.started = true;
            this._bindToTableStream();
        }
    }
    start(kafkaReadyCallback = null, kafkaErrorCallback = null, withBackPressure = false, outputKafkaConfig = null) {
        if (kafkaReadyCallback && typeof kafkaReadyCallback === "object" && arguments.length < 2) {
            return new Promise((resolve, reject) => {
                this._start(resolve, reject, kafkaReadyCallback.withBackPressure, kafkaReadyCallback.outputKafkaConfig);
            });
        }
        if (arguments.length < 2) {
            return new Promise((resolve, reject) => {
                this._start(resolve, reject, withBackPressure);
            });
        }
        return this._start(kafkaReadyCallback, kafkaErrorCallback, withBackPressure, outputKafkaConfig);
    }
    _start(kafkaReadyCallback = null, kafkaErrorCallback = null, withBackPressure = false, outputKafkaConfig = null) {
        if (this.started) {
            throw new Error("this KTable is already started.");
        }
        this.started = true;
        if (!this.topicName && !this.produceAsTopic) {
            return kafkaReadyCallback();
        }
        if (!this.finalised) {
            this.finalise();
        }
        let producerReady = false;
        let consumerReady = false;
        const onReady = (type) => {
            switch (type) {
                case "producer":
                    producerReady = true;
                    break;
                case "consumer":
                    consumerReady = true;
                    break;
            }
            if (producerReady && consumerReady && kafkaReadyCallback) {
                kafkaReadyCallback();
            }
            if (!this.produceAsTopic && consumerReady && kafkaReadyCallback) {
                kafkaReadyCallback();
            }
            if (this.produceAsTopic && producerReady && kafkaReadyCallback && !this.kafka.topic && !this.kafka.topic.length) {
                kafkaReadyCallback();
            }
        };
        this.kafka.overwriteTopics(this.topicName);
        this.kafka.on(MESSAGE, msg => {
            if (!this.consumerOpen) {
                return;
            }
            super.writeToStream(msg);
        });
        this.kafka.start(() => { onReady("consumer"); }, kafkaErrorCallback, this.produceAsTopic, withBackPressure);
        if (this.produceAsTopic) {
            this.kafka.setupProducer(this.outputTopicName, this.outputPartitionsCount, () => { onReady("producer"); }, kafkaErrorCallback, outputKafkaConfig);
            super.forEach(message => {
                messageProduceHandle(this.kafka, message, this.outputTopicName, this.produceType, this.produceCompressionType, this.produceVersion, kafkaErrorCallback);
            });
        }
    }
    innerJoin(stream, key = "key") {
        throw new Error("not implemented yet.");
    }
    outerJoin(stream) {
        throw new Error("not implemented yet.");
    }
    leftJoin(stream) {
        throw new Error("not implemented yet.");
    }
    writeToTableStream(message) {
        this._tee.emit(MESSAGE, message);
    }
    finalise(buildReadyCallback = null) {
        if (this.finalised) {
            throw new Error("this KTable has already been finalised.");
        }
        const lastState = new LastState(this.storage);
        this.asyncMap(lastState.execute.bind(lastState));
        this.stream$.forEach(NOOP).then(_ => {
            this.kafka.closeConsumer();
            this.consumerOpen = false;
            if (typeof buildReadyCallback === "function") {
                buildReadyCallback();
            }
        });
        this._bindToTableStream();
        this.finalised = true;
    }
    _bindToTableStream() {
        this.stream$ = most.merge(this.stream$, most.fromEvent(MESSAGE, this._tee));
    }
    consumeUntilMs(ms = 1000, finishedCallback = null) {
        super.multicast();
        this.stream$ = this.stream$.until(most.of().delay(ms));
        if (!this.finalised) {
            this.finalise(finishedCallback);
        }
        return this;
    }
    consumeUntilCount(count = 1000, finishedCallback = null) {
        super.multicast();
        this.stream$ = this.stream$.take(count);
        if (!this.finalised) {
            this.finalise(finishedCallback);
        }
        return this;
    }
    consumeUntilLatestOffset(finishedCallback = null) {
        throw new Error("not implemented yet.");
    }
    getTable() {
        return this.storage.getState();
    }
    replay() {
        Object.keys(this.storage.state).forEach(key => {
            const message = {
                key: key,
                value: this.storage.state[key]
            };
            this.writeToTableStream(message);
        });
    }
    merge(stream) {
        if (!(stream instanceof StreamDSL)) {
            throw new Error("stream has to be an instance of KStream or KTable.");
        }
        const newStream$ = this.stream$.multicast().merge(stream.stream$.multicast());
        return StorageMerger.mergeIntoState([this.getStorage(), stream.getStorage()]).then(mergedState => {
            return this._cloneWith(newStream$, mergedState);
        });
    }
    _cloneWith(newStream$, storageState = {}) {
        const kafkaStreams = this._kafkaStreams;
        if (!kafkaStreams) {
            throw new Error("merging requires a kafka streams reference on the left-hand merger.");
        }
        const newStorage = kafkaStreams.getStorage();
        const newKafkaClient = kafkaStreams.getKafkaClient();
        return newStorage.setState(storageState).then(_ => {
            const newInstance = new KTable(null, NOOP, newStorage, newKafkaClient, true);
            newInstance.replaceInternalObservable(newStream$);
            return newInstance;
        });
    }
    clone() {
        const newStream$ = this.stream$.tap(NOOP);
        return this._cloneWith(newStream$);
    }
    close() {
        this.stream$ = this.stream$.take(0);
        this.stream$ = null;
        this.kafka.close();
        return this.storage.close();
    }
}
module.exports = KTable;
//# sourceMappingURL=KTable.js.map