"use strict";
const Promise = require("bluebird");
const { async: createSubject } = require("most-subject");
const lodashClone = require("lodash.clone");
const lodashCloneDeep = require("lodash.clonedeep");
const StreamDSL = require("./StreamDSL.js");
const { Window } = require("../actions/index");
const { messageProduceHandle } = require("../messageProduceHandle.js");
const NOOP = () => { };
class KStream extends StreamDSL {
    constructor(topicName, storage = null, kafka = null, isClone = false) {
        super(topicName, storage, kafka, isClone);
        this.started = false;
        if (isClone) {
            this.started = true;
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
            throw new Error("this KStream is already started.");
        }
        this.started = true;
        if (this.noTopicProvided && !this.produceAsTopic) {
            return kafkaReadyCallback();
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
            if (this.produceAsTopic && producerReady && kafkaReadyCallback && !this.kafka.topic || !this.kafka.topic.length) {
                kafkaReadyCallback();
            }
        };
        this.kafka.overwriteTopics(this.topicName);
        this.kafka.on("message", msg => super.writeToStream(msg));
        this.kafka.start(() => { onReady("consumer"); }, kafkaErrorCallback || NOOP, this.produceAsTopic, withBackPressure);
        if (this.produceAsTopic) {
            this.kafka.setupProducer(this.outputTopicName, this.outputPartitionsCount, () => { onReady("producer"); }, kafkaErrorCallback, outputKafkaConfig);
            super.forEach(message => {
                messageProduceHandle(this.kafka, message, this.outputTopicName, this.produceType, this.produceCompressionType, this.produceVersion, kafkaErrorCallback);
            });
        }
    }
    innerJoin(stream, key = "key", windowed = false, combine = null) {
        let join$ = null;
        if (!windowed) {
            join$ = this._innerJoinNoWindow(stream, key, combine);
        }
        else {
            throw new Error("not implemented yet.");
        }
        return this._cloneWith(join$);
    }
    _innerJoinNoWindow(stream, key, combine) {
        const existingKeyFilter = (event) => {
            return !!event && typeof event === "object" && typeof event[key] !== "undefined";
        };
        const melt = (left, right) => {
            return {
                left,
                right
            };
        };
        const parent$ = super.multicast().stream$.filter(existingKeyFilter);
        const side$ = stream.multicast().stream$.filter(existingKeyFilter);
        return parent$.zip(combine || melt, side$);
    }
    outerJoin(stream) {
        throw new Error("not implemented yet.");
    }
    leftJoin(stream) {
        throw new Error("not implemented yet.");
    }
    merge(stream) {
        if (!(stream instanceof StreamDSL)) {
            throw new Error("stream has to be an instance of KStream or KTable.");
        }
        const newStream$ = this.stream$.multicast().merge(stream.stream$.multicast());
        return this._cloneWith(newStream$);
    }
    _cloneWith(newStream$) {
        const kafkaStreams = this._kafkaStreams;
        if (!kafkaStreams) {
            throw new Error("merging requires a kafka streams reference on the left-hand merger.");
        }
        const newStorage = kafkaStreams.getStorage();
        const newKafkaClient = kafkaStreams.getKafkaClient();
        const newInstance = new KStream(null, newStorage, newKafkaClient, true);
        newInstance.replaceInternalObservable(newStream$);
        newInstance._kafkaStreams = kafkaStreams;
        return newInstance;
    }
    fromMost(stream$) {
        return this._cloneWith(stream$);
    }
    clone(cloneEvents = false, cloneDeep = false) {
        let clone$ = this.stream$.multicast();
        if (cloneEvents) {
            clone$ = clone$.map((event) => {
                if (!cloneDeep) {
                    return lodashClone(event);
                }
                return lodashCloneDeep(event);
            });
        }
        return this._cloneWith(clone$);
    }
    branch(preds = []) {
        if (!Array.isArray(preds)) {
            throw new Error("branch predicates must be an array.");
        }
        return preds.map((pred) => {
            if (typeof pred !== "function") {
                throw new Error("branch predicates must be an array of functions: ", pred);
            }
            return this.clone(true, true).filter(pred);
        });
    }
    window(from, to, etl = null, encapsulated = true, collect = true) {
        if (typeof from !== "number" || typeof to !== "number") {
            throw new Error("from & to should be unix epoch ms times.");
        }
        let stream$ = null;
        if (!etl) {
            stream$ = this.stream$.timestamp();
        }
        else {
            stream$ = this.stream$.map(element => {
                return {
                    time: etl(element),
                    value: element
                };
            });
        }
        let aborted = false;
        const abort$ = createSubject();
        function abort() {
            if (aborted) {
                return;
            }
            aborted = true;
            abort$.next(null);
        }
        const window = new Window([], collect);
        const window$ = window.getStream();
        stream$
            .skipWhile(event => event.time < from)
            .takeWhile(event => event.time < to)
            .until(abort$)
            .tap(event => window.execute(event, encapsulated))
            .drain().then(_ => {
            window.writeToStream();
            window.flush();
        })
            .catch((error) => {
            window$.error(error);
        });
        return {
            window,
            abort,
            stream: this._cloneWith(window$)
        };
    }
    close() {
        this.stream$ = this.stream$.take(0);
        this.stream$ = null;
        this.kafka.close();
        return this.storage.close();
    }
}
module.exports = KStream;
//# sourceMappingURL=KStream.js.map