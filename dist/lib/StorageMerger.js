"use strict";
class StorageMerger {
    static mergeIntoState(storages) {
        return Promise.all(storages
            .map(storage => storage.getState()))
            .then(states => {
            const newState = {};
            states.forEach(state => {
                Object.keys(state).forEach(key => {
                    newState[key] = state[key];
                });
            });
            return newState;
        });
    }
}
module.exports = StorageMerger;
//# sourceMappingURL=StorageMerger.js.map