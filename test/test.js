
var assert = require ('assert');
var async = require ('async');
var mingydb = require ('../main');
var mongodb = require ('mongodb');

describe ("basic connectivity", function(){
    before (function (done) {
        var db = new mongodb.Db ('test-mingydb', new mongodb.Server ('127.0.0.1', 27017), { w:1 });
        db.open (function (err) {
            if (err) return done (err);
            async.each ([ 'test-mingydb', 'mins' ], function (dbname, callback) {
                db.collection (dbname, function (err, col) {
                    if (err) return callback (err);
                    col.remove ({}, { w:1, flush:true }, function (err) {
                        if (err) return callback (err);
                        col.dropAllIndexes (function (err) {
                            if (err && err.message != 'ns not found') return callback (err);
                            col.find ({}, function (err, cursor) {
                                if (err) return callback (err);
                                cursor.count (function (err, n) {
                                    if (err) return callback (err);
                                    if (n)
                                        return callback (new Error (
                                            'failed to delete records before test (found '
                                          + n
                                          + ' records)'
                                        ));
                                    callback();
                                });
                            });
                        });
                    });
                });
            }, done);
        });
    });

    var nextID = 1;
    function getNextID(){ return 'tempID_'+nextID++; }
    var testCollection;
    var rawTestCollection;

    it ("acquires a raw database", function (done) {
        mingydb.rawDatabase (
            'test-mingydb',
            new mongodb.Server ('127.0.0.1', 27017),
            function (err, db) {
                if (err) return done (err);

                done();
            }
        );
    });

    it ("acquires a raw collection", function (done) {
        mingydb.rawCollection (
            'test-mingydb',
            'test-mingydb',
            new mongodb.Server ('127.0.0.1', 27017),
            function (err, col) {
                if (err) return done (err);
                rawTestCollection = col;
                done();
            }
        );
    });

    it ("acquires a raw database with #rawConnect", function (done) {
        mingydb.rawConnect (
            'mongodb://127.0.0.1:27017/test-mingydb',
            function (err, client) {
                if (err) return done (err);
                done();
            }
        );
    });

    it ("acquires a raw client with #rawOpen", function (done) {
        mingydb.rawOpen (
            new mongodb.Server ('127.0.0.1', 27017),
            function (err, client) {
                if (err) return done (err);
                done();
            }
        );
    });

    it ("acquires a database", function (done) {
        mingydb.database (
            'test-mingydb',
            new mongodb.Server ('127.0.0.1', 27017),
            function (err, db) {
                if (err) return done (err);
                if (db.name !== 'test-mingydb')
                    return done (new Error ('Collection has no #name property'));
                done();
            }
        );
    });

    it ("acquires a collection", function (done) {
        mingydb.collection (
            'test-mingydb',
            'test-mingydb',
            new mongodb.Server ('127.0.0.1', 27017),
            function (err, col) {
                if (err) return done (err);
                testCollection = col;
                done();
            }
        );
    });

    it ("acquires a database with #connect", function (done) {
        mingydb.connect (
            'mongodb://127.0.0.1:27017/test-mingydb',
            function (err, client) {
                if (err) return done (err);
                done();
            }
        );
    });

    it ("acquires a client with #open", function (done) {
        mingydb.open (
            new mongodb.Server ('127.0.0.1', 27017),
            function (err, client) {
                if (err) return done (err);
                done();
            }
        );
    });
});

require ('./Database');
require ('./Compression');
require ('./Collection');
require ('./Aggregation');
