const async = require('async');
const EventEmitter = require('events');

/**
 * Base DBManager function.
 * @exports DBManager
 * @class
 * @constructor
 * @param {object} params
 */
function DBManager(params) {
    this._eventEmitter = new EventEmitter();
    this.params = Object.assign({
        // defaults
    }, params);
}

Object.assign(DBManager.prototype, {
    on() {
        return this._eventEmitter.on.apply(this._eventEmitter, arguments);
    },
    in() {
        return this._eventEmitter.in.apply(this._eventEmitter, arguments);
    },
    off() {
        return this._eventEmitter.off.apply(this._eventEmitter, arguments);
    },
    emit() {
        return this._eventEmitter.emit.apply(this._eventEmitter, arguments);
    },
    /**
     * Instantiates a dbManager object 
     * @memberOf DBManager.prototype
     * @param {object} _db The mongo db instance or mock
     * @param {object} mongoLib The mongo library
     */
    init (_db, mongoLib) {
        this.db=_db;
        this.ObjectID = mongoLib ? mongoLib.ObjectId : undefined;
        this.ObjectId = this.ObjectID;
        this.params.onReady && this.params.onReady({ db: _db });
    },
    /**
     * Ensures an index is present in a collection in the database.
     * @param {object} params The parameters for the operation: { collection(optional): String, fields: object, options.object }
     * @param {function} callback the callback
     */
    createIndex (params, callback) {
        const _params = Object.assign({}, this.params, params);
        const collection = _params.collection;
        const fields = _params.fields;
        const options = _params.options;
        return this.collection(collection).createIndex(fields, options, callback);

    },

    /**
     * Returns a plain array of distinct values for a query.
     * @param {object} params The parameters for the operation: {field: String, find: object, options: object}
     * @param {function} callback the callback
     */
    distinct (params, callback){
        return new Promise((resolve, reject) => {
            params = Object.assign({}, this.params, params);
            async.waterfall([
                cb => this.findFromParams(params, cb),
                (filter, next) => {
                    this.collection(params.collection).distinct(params.field, filter, params.options, next);
                }
            ], (err, response) => {
                callback(err, response);
                if (err) return reject(err);
                resolve(response);
            });
        });
    },
    /**
     * INTERNAL METHOD: method that performs the query and returns the mongo cursor.
     * @param {object} params The parameters for the Operation
     */
    _get (params) {
        let _params = Object.assign({}, this.params, params);
        let cursor = this.collection(_params.collection).find(_params.filter);
        if (_params.modifiers && Object.getOwnPropertyNames(_params.modifiers).length) {
            Object.getOwnPropertyNames(_params.modifiers).forEach((mod) => {
                cursor = cursor[mod](_params.modifiers[mod]);
            });
        }
        return cursor;
    },

    /**
     * Returns a collection instance
     * @memberOf DBManager.prototype
     * @function
     * @param {string} name Collection name
     */
    collection(name) {
        return this.db.collection(name || this.params.collection);
    },
    /**
     * Inserts a new document
     * @memberOf DBManager.prototype
     * @function
     * @param {object} payload Payload to insert
     * @param {callback} callback async handler
     * @async
     */
    insertItem(payload, callback) {
        this.collection().insertOne(payload, {w: 1}, (err, result) => {
            if (err) return callback(err, null);
            callback(null, result.ops[0]._id);
        });
    },
    /**
     * Inserts many documents
     * @memberOf DBManager.prototype
     * @function
     * @param {object} payload Payload to insert
     * @param {callback} callback async handler
     * @async
     */
    insertMany (payload, callback) {
        return this.collection().insertMany(payload, {w:1}, callback);
    },
    /**
     * Retrieve a single document based on params
     * @memberOf DBManager.prototype
     * @function
     * @param {params} params Parameters object
     * @param {callback} callback async handler
     * @async
     * @see {@link #DBManager+findFromParams}
     */
    getItem(params, callback) {
        params = params || {};
        async.waterfall([
            cb => this.findFromParams(params, cb),
            (find, cb) => {
                return this.collection().findOne(find, {}, cb);
            },
            (one, cb) => {
                if (!this.params.autoInsert || one) return cb(null, one);
                if (!one) {
                    return async.waterfall([
                        cb => this.findFromParams(params, cb),
                        (find, cb) => this.insertItem(find, (err) => {
                            if (err) return cb(err);
                            cb(null, find);
                        }),
                        (find, cb) => this.collection().findOne(find, {}, cb)
                    ], cb);
                }
                cb(null, one);
            },
            (one, cb) => {
                if (params.validate) return params.validate(one, cb);
                cb(null, one);
            }
        ], callback);
    },
    /**
     * Retrieve many documents based on params
     * @memberOf DBManager.prototype
     * @function
     * @param {params} params Parameters object
     * @param {callback} callback async handler
     * @async
     * @see {@link #DBManager+findFromParams}
     */
    getItems(params, callback) {
        return new Promise((resolve, reject) => {
            params = params || {};
            async.waterfall([
                cb => this.findFromParams(params, cb),
                (filter, cb) => {
                    if (params.sort || params.limit){
                        params.modifiers = params.modifiers || {};
                        if (params.sort) params.modifiers.sort = params.sort;
                        if (params.limit !== undefined) params.modifiers.limit = params.limit;
                    }
                    params.filter = filter;
                    let stream = this._get(params).stream();
                    let response = [];
                    let error = false;
                    stream.on("data", (doc) => {
                        if (params.onData) doc = params.onData(doc);
                        response.push(doc);
                    });
                    stream.on("error", (err) => {
                        error = true;
                        cb(err);
                    });
                    stream.on("end", () => {
                        !error && cb(null, response);
                    });
                }
            ], (err, response) => {
                callback && callback(err, response);
                if (err) return reject(err);
                return resolve(response);
            });
        });
    },
    /**
     * Returns a document value based on params.path
     * @memberOf DBManager.prototype
     * @function
     * @param {params} params Parameters object
     * @param {callback} callback async handler
     * @async
     * @see {@link #DBManager+findFromParams}
     */
    getItemPath(params, callback) {
        this.getItem(params, (err, item) => {
            if (err) {
                callback(err);
                return;
            }
            if (!item) {
                callback(null, undefined);
                return;
            }
            try {
                return callback(null, eval(`item.${params.path}`));
            } catch(e) {
                callback(null, undefined);
            }
        });
    },
    /**
     * Update a document value based on params.path
     * @memberOf DBManager.prototype
     * @function
     * @param {params} params Parameters object
     * @param {callback} callback async handler
     * @async
     * @see {@link #DBManager+findFromParams}
     */
    setItemPath(params, callback) {
        var updateParams = Object.assign({}, params, {
            $set: {
                [params.path]: params.value
            },
            flags: {
                upsert: true,
                safe: true
            }
        });
        this.updateItem(updateParams, callback);
    },
    /**
     * Delete a single document
     * @memberOf DBManager.prototype
     * @function
     * @param {params} params Parameters object
     * @param {callback} callback async handler
     * @async
     * @see {@link #DBManager+findFromParams}
     */
    deleteItem(params, callback) {
        return new Promise((resolve, reject) => {
            params = params || {};
            async.waterfall([
                cb => this.findFromParams(params, cb),
                (find, cb) => {
                    this.collection().deleteOne(find, {w: 1}, cb);
                },
                (result, cb) => {
                    if (this.params.onDeleteItem) {
                        return this.params.onDeleteItem.call(this, {
                            _id: this.castID(params.id),
                            find: Object.assign({}, params.find)
                        }, (err) => {
                            cb(err, result);
                        });
                    } else {
                        cb(null, result);
                    }
                }
            ], (err, res) => {
                callback && callback(err, res);
                if (err) return reject(err);
                resolve(res);
            });
        });
    },
    /**
     * Update a document payload
     * @memberOf DBManager.prototype
     * @function
     * @param {params} params Parameters object
     * @param {callback} callback async handler
     * @async
     * @see {@link #DBManager+findFromParams}
     */
    updateItem(params, callback) {
        return new Promise((resolve, reject) => {
            // params = params || {};
            params = Object.assign({}, this.params, params);
            if (params.autoInsert) {
                params.flags = Object.assign({}, params.flags, {upsert: true, safe: true});
            }
            async.waterfall([
                cb => this.findFromParams(params, cb),
                (find, cb) => {
                    if (params.autoInsert) return cb(null, find);
                    // TODO: Why ask? why not let it just explote and manage the rejection?
                    this.getItem(params, (err, item) => {
                        if (err) return cb(err);
                        if (!item) return (cb(new Error('no item found')));
                        cb(null, find);
                    });
                },
                (find, cb) => {
                    var updateData = {};
                    var flags = params.flags || { w: 1 };
                    if (flags.upsert) updateData.$set = Object.assign({}, updateData.$set, params.payload);
                    if (params.$set) updateData.$set = Object.assign({}, updateData.$set, params.$set);
                    if (params.$unset) updateData.$unset = params.$unset;
                    this.collection().updateOne(find, updateData, flags, cb);
                }
            ], (err, result) => {
                this.getItem(params, (err, item) => {
                    if (!err) this.emit('item:update', item);
                });
                callback && callback(err, result);
                if (err) return reject(err);
                resolve(result);
            });
        });
    },
    /**
     * Build a find object based on params
     * @memberOf DBManager.prototype
     * @function
     * @param {params} params Parameters object
     * @param {callback} callback async handler
     * @async
     */
    findFromParams(params, callback) {
        async.waterfall([
            cb => cb(null, Object.assign({}, params.find)),
            (find, cb) => {
                if (params.id) return this.castID(params.id, (err, _id) => {
                    if (err) return cb(err);
                    cb(null, Object.assign(find, {
                        _id
                    }));
                });
                if (params.ids) return this.castIDs(params.ids, (err, $in) => {
                    if (err) return cb(err);
                    cb(null, Object.assign(find, {
                        _id: { $in }
                    }));
                });
                cb(null, find);
            }
        ], callback);
    },
    /**
     * Casts an arrays of IDs to ObjectID instances
     * @memberOf DBManager.prototype
     * @function
     * @param {Array<string>} ids IDs
     * @param {callback} callback async handler
     * @async
     */
    castIDs(ids, cb) {
        try {
            var casted = ids.map(id => this.castID(id));
            if (!cb) return casted;
            cb(null, casted);
        } catch (e) {
            if (!cb) throw e;
            cb(e);
        }
    },
    /**
     * Casts a single ID to an ObjectID instance
     * @memberOf DBManager.prototype
     * @function
     * @param {string} id ID
     * @param {callback} callback async handler
     * @async
     */
    castID(id, cb) {
        if (typeof id !== 'string') {
            if (!cb) return id;
            return cb(null, id);
        }
        try {
            var casted = this.ObjectID(id);
            if (!cb) return casted;
            cb(null, this.ObjectID(id));
        } catch (e) {
            var err = new Error(`invalid id '${id}'`);
            if (!cb) throw err;
            cb(err);
        }
    }
});

module.exports = DBManager;
