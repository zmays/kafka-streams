"use strict";
const { async: createSubject } = require("most-subject");
class Window {
    constructor(container, collect = false) {
        this.container = container || [];
        this.container$ = createSubject();
        this.collect = collect;
    }
    getStream() {
        return this.container$;
    }
    execute(element, leaveEncapsulated = true) {
        const ele = leaveEncapsulated ? element : element.value;
        if (this.collect) {
            this.container.push(ele);
        }
        else {
            this.container$.next(ele);
        }
    }
    writeToStream() {
        if (this.collect) {
            this.container.forEach(event => this.container$.next(event));
        }
        else {
        }
    }
    flush() {
        this.container$.complete();
    }
}
module.exports = Window;
//# sourceMappingURL=Window.js.map