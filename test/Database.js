
var assert = require ('assert');
var async = require ('async');
var mingydb = require ('../main');
var mongodb = require ('mongodb');

describe ("Database", function(){
    var nextID = 1;
    function getNextID(){ return 'tempID_'+nextID++; }
    var testCollection;
    var rawTestCollection;
    before (function (done) {
        var db = new mongodb.Db ('test-mingydb', new mongodb.Server ('127.0.0.1', 27017), { w:1 });
        db.open (function (err) {
            if (err) return done (err);
            async.each ([ 'test-mingydb', '_mins' ], function (dbname, callback) {
                db.collection (dbname, function doRemove (err, col) {
                    if (err) return callback (err);
                    col.remove ({}, { w:1 }, function (err) {
                        if (err) return callback (err);
                        col.dropAllIndexes (function (err) {
                            if (err && err.message != 'ns not found') return callback (err);
                            col.find ({}, function (err, cursor) {
                                if (err) return callback (err);
                                cursor.count (function (err, n) {
                                    if (err) return callback (err);
                                    if (n)
                                        return doRemove (undefined, col);
                                        // return cursor.toArray (function (err, recs) {
                                        //     console.log (recs);
                                        //     callback (new Error ('failed to delete records'));
                                        // });

                                    async.parallel ([
                                        function (callback) {
                                            mingydb.collection (
                                                'test-mingydb',
                                                'test-mingydb',
                                                new mongodb.Server ('127.0.0.1', 27017),
                                                function (err, col) {
                                                    if (err) return callback (err);
                                                    testCollection = col;
                                                    callback();
                                                }
                                            );
                                        },
                                        function (callback) {
                                            mingydb.rawCollection (
                                                'test-mingydb',
                                                'test-mingydb',
                                                new mongodb.Server ('127.0.0.1', 27017),
                                                function (err, col) {
                                                    if (err) return callback (err);
                                                    rawTestCollection = col;
                                                    callback();
                                                }
                                            );
                                        }
                                    ], callback);
                                });
                            });
                        });
                    });
                });
            }, done);
        });
    });

    this.timeout (150); // sometimes Mongo's first op in a while is very slow

    it ("closes a database connection", function (done) {
        mingydb.database (
            'test-mingydb',
            new mongodb.Server ('127.0.0.1', 27017),
            function (err, db) {
                if (err) return done (err);
                db.close (done);
            }
        );
    });

    it ("creates more db instances with #db", function (done) {
        mingydb.database (
            'test-mingydb',
            new mongodb.Server ('127.0.0.1', 27017),
            function (err, db) {
                if (err) return done (err);
                var newdb = db.db ('another_db');
                if (!(newdb instanceof mingydb.Database))
                    return done (new Error (
                        'returned instance is not a mingydb.Database'
                    ));
                done();
            }
        );
    });

    it ("accesses the admin database with #admin", function (done) {
        mingydb.database (
            'test-mingydb',
            new mongodb.Server ('127.0.0.1', 27017),
            function (err, db) {
                if (err) return done (err);
                db.admin (done);
            }
        );
    });

    it ("lists Collection info with #listCollections", function (done) {
        mingydb.database (
            'test-mingydb',
            new mongodb.Server ('127.0.0.1', 27017),
            function (err, db) {
                if (err) return done (err);
                var cursor = db.listCollections();
                cursor.toArray (function (err, colDocs) {
                    if (err) return done (err);
                    if (colDocs.length <= 1)
                        return done (new Error (
                            'did not retrieve enough collection info docs (' + colDocs.length + ')'
                        ));
                    done();
                });
            }
        );
    });

    it ("lists Collection info with #listCollections through a filter query", function (done) {
        mingydb.database (
            'test-mingydb',
            new mongodb.Server ('127.0.0.1', 27017),
            function (err, db) {
                if (err) return done (err);
                var cursor = db.listCollections ({ name:/[^.]*\.mins/ });
                cursor.toArray (function (err, colDocs) {
                    if (err) return done (err);
                    if (colDocs.length != 1)
                        return done (new Error (
                            'retrieved incorrect number of collection info docs'
                        ));
                    done();
                });
            }
        );
    });

    it ("lists Collection names with #collectionNames", function (done) {
        mingydb.database (
            'test-mingydb',
            new mongodb.Server ('127.0.0.1', 27017),
            function (err, db) {
                if (err) return done (err);
                var cursor = db.listCollections ({ name:/[^.]*\.mins/ });
                cursor.toArray (function (err, colDocs) {
                    if (err) return done (err);
                    if (colDocs.length != 1)
                        return done (new Error (
                            'retrieved incorrect number of collection info docs'
                        ));
                    done();
                });
            }
        );
    });

    it ("acquires a Collection instance from the module level with .collection", function (done) {
        mingydb.collection (
            'test-mingydb',
            'test-mingydb',
            new mongodb.Server ('127.0.0.1', 27017),
            function (err, col) {
                if (err) return done (err);
                if (!(col instanceof mingydb.Collection))
                    return done (new Error (
                        'returned instance is not a mingydb.Collection'
                    ));
                done();
            }
        );
    });

    it ("acquires a Collection instance from a Database instance", function (done) {
        mingydb.database (
            'test-mingydb',
            new mongodb.Server ('127.0.0.1', 27017),
            function (err, db) {
                if (err) return done (err);
                db.collection ('test-mingydb', function (err, col) {
                    if (err) return done (err);
                    if (!(col instanceof mingydb.Collection))
                        return done (new Error (
                            'returned instance is not a mingydb.Collection'
                        ));
                    done();
                });
            }
        );
    });

    it ("acquires every Collection instance with #collections", function (done) {
        mingydb.database (
            'test-mingydb',
            new mongodb.Server ('127.0.0.1', 27017),
            function (err, db) {
                if (err) return done (err);
                db.collections (function (err, cols) {
                    if (err) return done (err);
                    if (!cols || !(cols instanceof Array))
                        return done (new Error (
                            'did not return an Array'
                        ));
                    if (cols.length <= 1)
                        return done (new Error (
                            'did not return enough Collections'
                        ));
                    for (var i in cols)
                        if (!(cols[i] instanceof mingydb.Collection))
                            return done (new Error (
                                'returned instance is not a mingydb.Collection'
                            ));
                    done();
                });
            }
        );
    });
});
