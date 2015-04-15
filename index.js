'use strict';

var Cassandra = require('cassandra-driver');

var Batcher = function(db, opts) {
    opts = opts || {};
    this.batchSize = opts.batchSize || 5000;
    this.autoSend  = opts.autoSend === false ? false : true;
    if (db.contactPoints) {
        this.db = new Cassandra.Client(db);
    } else {
        this.db = db;
    }

    this.info = {inserts: 0, errors: 0};
    this.toInsert = 0;
    this.queries = [];
    this.debug = opts.debug === false ? false : true;
    if (this.debug) {
        console.time('cassandraBatcher');
    }
};

Batcher.prototype._checkBatch = function() {
    return this.toInsert >= this.batchSize;
};

Batcher.prototype._sendBatch = function() {
    this.db.batch(this.queries, {prepare: true}, function (err) {
        if (err) {
            console.log(err.stack);
            this.info.errors++;
        } else {
            this.info.inserts++;
        }

        if (this.debug && this.info.inserts % 20 == 0) {
            console.log(this.info);
        }
    }.bind(this));

    this.toInsert = 0;
    this.queries = [];
};


Batcher.prototype.push = function(data) {
    ++this.toInsert;
    this.queries.push(data);
    if (this.autoSend && this._checkBatch()) {
        this._sendBatch();
    }
};

Batcher.prototype.send = function() {
    this._sendBatch();
};

Batcher.prototype.end = function() {
    if (this._checkBatch()) {
        this._sendBatch();
    }

    if (this.debug) {
        console.timeEnd('cassandraBatcher');
    }
};

module.exports = Batcher;
