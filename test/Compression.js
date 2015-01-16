
var assert = require ('assert');
var async = require ('async');
var mingydb = require ('../main');
var mongodb = require ('mongodb');

describe ("compression", function(){
    var nextID = 1;
    function getNextID(){ return 'tempID_'+nextID++; }
    var testCollection;
    var rawTestCollection;
    before (function (done) {
        var db = new mongodb.Db ('test-mingydb', new mongodb.Server ('127.0.0.1', 27017), { w:1 });
        db.open (function (err) {
            if (err) return done (err);
            async.each ([ 'test-mingydb', '_mins' ], function (dbname, callback) {
                db.collection (dbname, function (err, col) {
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
                                        return cursor.toArray (function (err, recs) {
                                            console.log (recs);
                                            callback (new Error ('failed to delete records'));
                                        });

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

    var testCompressor;
    var testDoc;
    it ("instantiates and initializes a Compressor", function (done) {
        testCompressor = new mingydb.Compressor ('test-mingydb', 'test-mingydb');
        testCompressor.ready (done);
    });

    var recoveryTestGoal;
    it ("compresses a document with novel keys", function (done) {
        testCompressor.compressRecord (recoveryTestGoal = {
            able:       {
                able:       {
                    able:       {

                    },
                    baker:      {

                    },
                    charlie:    9001
                },
                baker:      {
                    able:       {

                    },
                    baker:      {

                    },
                    charlie:    9001
                },
                charlie:    9001
            },
            baker:      {
                able:       {
                    able:       {

                    },
                    baker:      {

                    },
                    charlie:    9001
                },
                baker:      {
                    able:       {

                    },
                    baker:      {

                    },
                    charlie:    9001
                },
                charlie:    9001
            },
            charlie:    9001
        }, function (err, compressed) {
            if (err) return done (err);
            testDoc = compressed;
            done()
        });
    });

    it ("recovers the compressed document", function (done) {
        testCompressor.decompress (testDoc, function (err, decompressed) {
            if (err) return done (err);
            try {
                assert.deepEqual (recoveryTestGoal, decompressed, 'failed to recover document');
            } catch (err) { return done (err); }
            done();
        });
    });

    it ("compresses the document the same way every time", function (done) {
        if (!testDoc)
            return process.nextTick (function(){ return done (new Error (
                'could not proceeded - previous test failed'
            )); });
        async.timesSeries (100, function (n, callback) {
            testCompressor.compressRecord ({
                able:       {
                    able:       {
                        able:       {

                        },
                        baker:      {

                        },
                        charlie:    9001
                    },
                    baker:      {
                        able:       {

                        },
                        baker:      {

                        },
                        charlie:    9001
                    },
                    charlie:    9001
                },
                baker:      {
                    able:       {
                        able:       {

                        },
                        baker:      {

                        },
                        charlie:    9001
                    },
                    baker:      {
                        able:       {

                        },
                        baker:      {

                        },
                        charlie:    9001
                    },
                    charlie:    9001
                },
                charlie:    9001
            }, function (err, compressed) {
                if (err) return callback (err);
                try {
                    assert.deepEqual (testDoc, compressed);
                } catch (err) {
                    return callback (err);
                }
                callback()
            });
        }, done);
    });

    var arrTestDoc, arrTestGoal;
    it ("compresses a document with an Array of subdocuments", function (done) {
        if (!testDoc)
            return process.nextTick (function(){ return done (new Error (
                'could not proceeded - previous test failed'
            )); });

        testCompressor.compressRecord (arrTestGoal = {
            test:       'withArrays',
            able:       [
                {
                    able:       [
                        {
                            able:       {

                            },
                            baker:      {

                            },
                            charlie:    9001
                        }
                    ],
                    baker:      [
                        {
                            able:       {

                            },
                            baker:      {

                            },
                            charlie:    9001
                        }
                    ],
                    charlie:    9001
                }
            ],
            baker:      [
                {
                    able:       [
                        {
                            able:       {

                            },
                            baker:      {

                            },
                            charlie:    9001
                        }
                    ],
                    baker:      [
                        {
                            able:       {

                            },
                            baker:      {

                            },
                            charlie:    9001
                        }
                    ],
                    charlie:    9001
                }
            ],
            charlie:    9001
        }, function (err, compressed) {
            if (err) return done (err);
            arrTestDoc = compressed;
            done();
        });
    });

    it ("correctly decompresses an Array of subdocuments", function (done) {
        testCompressor.decompress (arrTestDoc, function (err, decompressed) {
            if (err) return done (err);
            try {
                assert.deepEqual (decompressed, arrTestGoal, 'failed to recover document');
            } catch (err) { return done (err); }
            done();
        });
    });

});
