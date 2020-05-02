"use strict";
const Promise = require("bluebird");
class LastState {
    constructor(storage, key = "key", fieldName = "value") {
        this.storage = storage;
        this.key = key;
        this.fieldName = fieldName;
    }
    execute(element) {
        if (!element || typeof element[this.key] === "undefined") {
            return Promise.resolve(element);
        }
        return this.storage.set(element[this.key], element[this.fieldName]).then(value => {
            return element;
        });
    }
}
module.exports = LastState;
//# sourceMappingURL=LastState.js.map