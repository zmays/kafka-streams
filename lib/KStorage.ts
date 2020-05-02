import {Storage} from "kafka-streams"

export default class KStorage implements Storage {
    options?: object;
    state: object

    /**
     * be aware that even though KStorage is built on Promises
     * its operations must always be ATOMIC (or ACID) because
     * the stream will access them parallel, therefore having
     * an async get + async set operation will always yield
     * in a large amount of missing get operations followed by
     * set operations
     */
    constructor(options?: object) {
        this.options = options;
        this.state = {};
    }

    /* NOTE: there is no open() method, meaning the functions have to work lazily */

    set(key: string, value: any): Promise<any> {
        this.state[key] = value;
        return Promise.resolve(value);
    }

    setSmaller(key = "min", value: any): Promise<any> {

        if (!this.state[key]) {
            this.state[key] = value;
        }

        if (value < this.state[key]) {
            this.state[key] = value;
        }

        return Promise.resolve(this.state[key]);
    }

    setGreater(key = "max", value: any): Promise<any> {

        if (!this.state[key]) {
            this.state[key] = value;
        }

        if (value > this.state[key]) {
            this.state[key] = value;
        }

        return Promise.resolve(this.state[key]);
    }

    increment(key: string, by: number = 1): Promise<number> {
        if (!this.state[key]) {
            this.state[key] = by;
        } else {
            this.state[key] += by;
        }
        return Promise.resolve(this.state[key]);
    }

    sum(key: string, value: number): Promise<any> {
        return this.increment(key, value);
    }

    get(key: string): Promise<any> {
        return Promise.resolve(this.state[key]);
    }

    getState(): Promise<object> {
        return Promise.resolve(this.state);
    }

    setState(newState: object): Promise<boolean> {
        this.state = newState;
        return Promise.resolve(true);
    }

    getMin(key: string = "min"): Promise<any> {
        return Promise.resolve(this.state[key]);
    }

    getMax(key: string = "max"): Promise<any> {
        return Promise.resolve(this.state[key]);
    }

    close(): Promise<boolean> {
        return Promise.resolve(true);
    }
}