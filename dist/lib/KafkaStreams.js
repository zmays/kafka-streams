"use strict";
const EventEmitter = require("events");
const KafkaFactory = require("./KafkaFactory.js");
const KStream = require("./dsl/KStream.js");
const KTable = require("./dsl/KTable.js");
const KStorage = require("./KStorage.js");
class KafkaStreams extends EventEmitter {
    constructor(config, storageClass = null, storageOptions = {}, disableStorageTest = false) {
        super();
        this.config = config;
        if (!this.config || typeof this.config !== "object") {
            throw new Error("Config must be a valid object.");
        }
        this.factory = new KafkaFactory(this.config, this.config.batchOptions);
        this.storageClass = storageClass || KStorage;
        this.storageOptions = storageOptions;
        this.kafkaClients = [];
        this.storages = [];
        if (!disableStorageTest) {
            KafkaStreams.checkStorageClass(this.storageClass);
        }
    }
    static checkStorageClass(storageClass) {
        let test = null;
        try {
            test = new storageClass();
        }
        catch (_) {
            throw new Error("storageClass should be a constructor.");
        }
        if (!(test instanceof KStorage)) {
            throw new Error("storageClass should be a constructor that extends KStorage.");
        }
    }
    getKafkaClient(topic) {
        const client = this.factory.getKafkaClient(topic);
        this.kafkaClients.push(client);
        return client;
    }
    getStorage() {
        const storage = new this.storageClass(this.storageOptions);
        this.storages.push(storage);
        return storage;
    }
    getKStream(topic, storage = null) {
        const kstream = new KStream(topic, storage || this.getStorage(), this.getKafkaClient(topic));
        kstream.setKafkaStreamsReference(this);
        return kstream;
    }
    fromMost(stream$, storage = null) {
        const kstream = this.getKStream(null, storage);
        kstream.replaceInternalObservable(stream$);
        return kstream;
    }
    getKTable(topic, keyMapETL, storage = null) {
        const ktable = new KTable(topic, keyMapETL, storage || this.getStorage(), this.getKafkaClient(topic));
        ktable.setKafkaStreamsReference(this);
        return ktable;
    }
    getStats() {
        return this.kafkaClients.map(kafkaClient => kafkaClient.getStats());
    }
    closeAll() {
        return Promise.all(this.kafkaClients.map(client => {
            return new Promise(resolve => {
                client.close();
                setTimeout(resolve, 750, true);
            });
        })).then(() => {
            this.kafkaClients = [];
            return Promise.all(this.storages.map(storage => storage.close())).then(() => {
                this.storages = [];
                super.emit("closed");
                return true;
            });
        });
    }
}
module.exports = KafkaStreams;
//# sourceMappingURL=KafkaStreams.js.map