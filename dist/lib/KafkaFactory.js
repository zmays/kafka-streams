"use strict";
const debug = require("debug")("kafka-streams:kafkafactory");
const JSKafkaClient = require("./client/JSKafkaClient.js");
const NativeKafkaClient = require("./client/NativeKafkaClient.js");
class KafkaFactory {
    constructor(config, batchOptions = undefined) {
        if (!config) {
            throw new Error("kafka factory constructor expects a configuration object.");
        }
        this.config = config;
        this.batchOptions = batchOptions;
    }
    getKafkaClient(topic) {
        if (this.config.noptions && typeof this.config.noptions === "object") {
            debug("creating new native kafka client");
            return new NativeKafkaClient(topic, this.config, this.batchOptions);
        }
        if (this.batchOptions) {
            debug("WARNING: batchOptions are omitted for the JS client.");
        }
        debug("creating new js kafka client");
        return new JSKafkaClient(topic, this.config);
    }
}
module.exports = KafkaFactory;
//# sourceMappingURL=KafkaFactory.js.map