"use strict";
const Promise = require("bluebird");
class KStorage {
    constructor(options) {
        this.options = options;
        this.state = {};
    }
    set(key, value) {
        this.state[key] = value;
        return Promise.resolve(value);
    }
    setSmaller(key = "min", value) {
        if (!this.state[key]) {
            this.state[key] = value;
        }
        if (value < this.state[key]) {
            this.state[key] = value;
        }
        return Promise.resolve(this.state[key]);
    }
    setGreater(key = "max", value) {
        if (!this.state[key]) {
            this.state[key] = value;
        }
        if (value > this.state[key]) {
            this.state[key] = value;
        }
        return Promise.resolve(this.state[key]);
    }
    increment(key, by = 1) {
        if (!this.state[key]) {
            this.state[key] = by;
        }
        else {
            this.state[key] += by;
        }
        return Promise.resolve(this.state[key]);
    }
    sum(key, value) {
        return this.increment(key, value);
    }
    get(key) {
        return Promise.resolve(this.state[key]);
    }
    getState() {
        return Promise.resolve(this.state);
    }
    setState(newState) {
        this.state = newState;
        return Promise.resolve(true);
    }
    getMin(key = "min") {
        return Promise.resolve(this.state[key]);
    }
    getMax(key = "max") {
        return Promise.resolve(this.state[key]);
    }
    close() {
        return Promise.resolve(true);
    }
}
module.exports = KStorage;
//# sourceMappingURL=KStorage.js.map