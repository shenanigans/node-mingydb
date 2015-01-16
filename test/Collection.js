
var assert = require ('assert');
var async = require ('async');
var mingydb = require ('../main');
var mongodb = require ('mongodb');

var typeGetter = ({}).toString;
function getTypeStr (obj) {
    var tstr = typeGetter.apply(obj).slice(8,-1).toLowerCase();
    if (tstr == 'object')
        if (obj instanceof Buffer) return 'buffer';
        else return tstr;
    if (tstr == 'text') return 'textnode';
    if (tstr == 'comment') return 'commentnode';
    if (tstr.slice(0,4) == 'html') return 'element';
    return tstr;
}

function matchLeaves (able, baker, arraysAsSets) {
    if (able === baker) return true;

    var aType = getTypeStr (able);
    var bType = getTypeStr (baker);
    if (aType != bType) return false;
    if (aType == 'array') {
        if (able.length != baker.length) return false;
        if (!arraysAsSets) {
            for (var i in able)
                if (!matchLeaves (able[i], baker[i], arraysAsSets))
                    return false;
            return true;
        }
        for (var i in able) {
            var found = false;
            for (var j in baker) {
                if (matchLeaves (able[i], baker[j], arraysAsSets)) {
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }
        return true;
    } else if (aType == 'object') {
        var keys = Object.keys (able);
        if (keys.length != Object.keys (baker).length) return false;
        for (var i in keys) {
            var key = keys[i];
            if (
                !Object.hasOwnProperty.call (baker, key)
             || !matchLeaves (able[key], baker[key], arraysAsSets)
            )
                return false;
        }
        return true;
    } else return false;
}

describe ("Collection", function(){
    this.timeout (500); // sometimes Mongo's first op in a while is very slow

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
                                            return callback (new Error ('failed to delete records'));
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

    it ("creates a Collection representation", function (done) {
        mingydb.collection (
            'test-mingydb',
            'test-mingydb',
            new mongodb.Server ('127.0.0.1', 27017),
            function (err, col) {
                if (err) return done (err);
                if (col.name !== 'test-mingydb')
                    return done (new Error ('Collection has no #name property'));
                testCollection = col;
                done();
            }
        );
    });

    describe ("#insert", function(){
        it ("inserts a record with novel keys", function (done) {
            testCollection.insert (
                {
                    able:       1,
                    baker:      2,
                    charlie:    'three',
                    dog:        {
                        able:       'one',
                        baker:      'two',
                        charlie:    3,
                        dog:        {
                            able:       9001
                        }
                    }
                },
                { w:1 },
                done
            );
        });
        it ("inserts a record with known keys", function (done) {
            testCollection.insert (
                {
                    able:       1,
                    baker:      2,
                    charlie:    'three',
                    dog:        {
                        able:       'one',
                        baker:      'two',
                        charlie:    3,
                        dog:        {
                            able:       9001
                        }
                    }
                },
                { w:1 },
                done
            );
        });

        it ("inserts a record with mixed novel and known keys", function (done) {
            testCollection.insert (
                {
                    able:       1,
                    baker:      2,
                    charlie:    'three',
                    dog:        {
                        able:       'one',
                        baker:      'two',
                        charlie:    3,
                        dog:        {
                            able:       9001
                        }
                    },
                    easy:       4,
                    fox:        5,
                    george:     {
                        easy:       4,
                        fox:        5,
                        george:     {
                            easy:       4,
                            fox:        5,
                            george:     'six'
                        }
                    }
                },
                { w:1 },
                done
            );
        });

        it ("survives a storm of mixed parallel insertions", function (done) {
            this.timeout (4000);
            var keys = [ 'jim', 'joe', 'jeff', 'paul', 'peter', 'chris', 'charles', 'ruby' ];

            function randomDoc (level) {
                level = level || 0;
                if (level > 5) return {};
                var doc = { _id:getNextID() };
                for (var i=0; i<4; i++) {
                    if (Math.random() < 0.33)
                        doc[keys[Math.floor (keys.length * Math.random())]] = randomDoc (level + 1);
                    else
                        doc[keys[Math.floor (keys.length * Math.random())]] = 100 * Math.random();
                }
                return doc;
            }

            var goal = 64;
            var complete = 0;
            var killed = false;
            for (var i=0,j=10; i<j; i++) (function insertRandomDoc(){
                var doc = randomDoc();
                var check = JSON.stringify (doc);
                testCollection.insert (doc, { w:1 }, function (err) {
                    if (killed) return;
                    if (err) {
                        killed = true;
                        return done (err);
                    }
                    testCollection.findOne ({ _id:doc._id }, function (err, rec) {
                        if (killed) return;
                        if (err) {
                            killed = true;
                            return done (err);
                        }
                        if (!rec) {
                            killed = true;
                            return done (new Error ('failed to retrieve record after insertion'));
                        }
                        if (!matchLeaves (rec, doc)) {
                            killed = true;
                            return done (new Error (
                                'record retrieved after insertion did not match'
                            ));
                        }

                        complete++;
                        if (complete < goal)
                            return process.nextTick (insertRandomDoc);
                        killed = true;
                        done();
                    });
                });
            })();
        });

    });

    describe ("#ensureIndex", function(){
        it ("creates an index", function (done) {
            testCollection.ensureIndex ({ xray:-1, zebra:1 }, function (err) {
                if (err) return done (err);
                testCollection.find (
                    { xray:2, zebra:2, test:'find' },
                    function (err, cursor) {
                        if (err) return done (err);
                        cursor.explain (function (err, info) {
                            if (err) return done (err);
                            if (!info) return done (new Error (
                                'did not retrieve any query information'
                            ));
                            if (info.cursor.slice (0, 11) != "BtreeCursor")
                                return done (new Error ('test query did not use index'));
                            done();
                        });
                    }
                );
            });
        });
    });

    describe ("#dropIndex", function(){
        it ("drops an index", function (done) {
            testCollection.dropIndex ('xray_-1_zebra_1', function (err) {
                if (err) return done (err);
                testCollection.find (
                    { xray:2, zebra:2, test:'find' },
                    function (err, cursor) {
                        if (err) return done (err);
                        cursor.explain (function (err, info) {
                            if (err) return done (err);
                            if (!info) return done (new Error (
                                'did not retrieve any query information'
                            ));
                            if (info.cursor.slice (0, 11) != "BasicCursor")
                                return done (new Error ('index was not deleted!'));
                            done();
                        });
                    }
                );
            });
        });
    });

    describe ("#dropAllIndexes", function(){
        it ("drops all indexes", function (done) {
            async.each ([ 'top', 'right', 'bottom', 'left' ], function (key, callback) {
                var indexDef = {};
                indexDef[key] = 1;
                testCollection.ensureIndex (indexDef, callback);
            }, function (err) {
                if (err) return done (err);
                testCollection.dropAllIndexes (function (err) {
                    if (err) return done (err);
                    async.each ([ 'top', 'right', 'bottom', 'left' ], function (key, callback) {
                        var query = {};
                        query[key] = 4;
                        testCollection.find (query, function (err, cursor) {
                            if (err) return callback (err);
                            cursor.explain (function (err, info) {
                                if (err) return callback (err);
                                if (!info) return callback (new Error (
                                    'did not retrieve any query information'
                                ));
                                if (info.cursor.slice (0, 11) != "BasicCursor")
                                    return callback (new Error (
                                        'index ' + info.cursor.slice (12) + ' was not deleted!'
                                    ));
                                callback();
                            });
                        });
                    }, done);
                });
            });
        });
    });

    var indexNames = {
        "_id_":                         true,
        "able_1_cheddar_-1_baker_1":    true,
        "able_1_gouda_-1_baker_1":      true,
        "able_1_edam_-1_baker_1":       true,
        "able_1_stilton_-1_baker_1":    true,
        "able_1_bris_-1_baker_1":       true,
        "parmesan":                     true
    };
    describe ("#listIndexes IndexCursor", function(){

        before (function (done) {
            async.each (
                [ 'cheddar', 'gouda', 'edam', 'stilton', 'bris' ],
                function (cheeseType, callback) {
                    var spec = { able:1 };
                    spec[cheeseType] = -1;
                    spec.baker = 1;
                    testCollection.ensureIndex (spec, callback);
                },
                function (err) {
                    if (err) return done (err);
                    testCollection.ensureIndex (
                        { able:1, parmesan:-1 },
                        { name:'parmesan', w:1 },
                        done
                    );
                }
            );
        });

        it ("lists all indexes with #nextObject", function (done) {
            var cursor = testCollection.listIndexes();
            var specs = [];
            var cancelled = false;
            cursor.nextObject (function cursorReactor (err, spec) {
                if (cancelled) return;
                if (err) return done (err);

                if (!spec) {
                    if (specs.length != 7)
                        return done (new Error (
                            'retrieved wrong number of index specifications ('+specs.length+')'
                        ));
                    return done();
                }

                if (!Object.hasOwnProperty.call (indexNames, spec.name)) {
                    cancelled = true;
                    return done (new Error (
                        'incorrect index name '+spec.name
                    ));
                }

                specs.push (spec);
                cursor.nextObject (cursorReactor);
            });
        });

        it ("lists all indexes with #toArray", function (done) {
            var cursor = testCollection.listIndexes();
            cursor.toArray (function (err, indexDocs) {
                if (err) return done (err);
                if (!indexDocs || !(indexDocs instanceof Array))
                    return done (new Error (
                        'did not retrieve an array'
                    ));
                if (indexDocs.length != 7)
                    return done (new Error (
                        'retrieved wrong number of index specifications ('+indexDocs.length+')'
                    ));
                for (var i in indexDocs)
                    if (!Object.hasOwnProperty.call (indexNames, indexDocs[i].name))
                        return done (new Error (
                            'incorrect index name '+indexDocs[i].name
                        ));
                done();
            });
        });

        it ("lists all indexes with #each", function (done) {
            var cursor = testCollection.listIndexes();
            var specs = [];
            var cancelled = false;
            cursor.each (function (err, spec) {
                if (cancelled) return;
                if (err) return done (err);

                if (!spec) {
                    if (specs.length != 7)
                        return done (new Error (
                            'retrieved wrong number of index specifications ('+specs.length+')'
                        ));
                    return done();
                }

                if (!Object.hasOwnProperty.call (indexNames, spec.name)) {
                    cancelled = true;
                    return done (new Error (
                        'incorrect index name '+spec.name
                    ));
                }
                specs.push (spec);
            });
        });
    });

    describe ("#indexInformation", function(){

        it ("provides index information with and without converted names", function (done) {
            testCollection.indexInformation (function (err, info) {
                if (err) return done (err);
                for (var name in info)
                    if (!Object.hasOwnProperty.call (indexNames, name))
                        return done (new Error ('information Object contained invalid index name'));
                done();
            });
        });
    });

    describe ("#indexExists", function(){

        it ("confirms existence of a name-converted index", function (done) {
            testCollection.indexExists ([ 'able_1_stilton_-1_baker_1' ], function (err, exists) {
                if (err) return done (err);
                if (!exists)
                    return done (new Error ('index not found'));
                done();
            });
        });

        it ("confirms existence of a named index without converting", function (done) {
            testCollection.indexExists ([ 'parmesan' ], function (err, exists) {
                if (err) return done (err);
                if (!exists)
                    return done (new Error ('index not found'));
                done();
            });
        });

    });

    describe ("#find", function(){

        var cursor;
        it ("retrieves a cursor for a query", function (done) {
            async.each ([ 0, 1, 2, 3, 4, 5, 6, 7 ], function (i, callback) {
                testCollection.insert (
                    {
                        _id:    getNextID(),
                        able:   i,
                        test:   "find"
                    },
                    { w:1 },
                    callback
                );
            }, function (err) {
                if (err) return done (err);
                testCollection.find (
                    { test:'find', able:{ $mod:[ 2, 1 ] }},
                    function (err, cursor) {
                        if (err) return done (err);
                        done();
                    }
                );
            });
        });

        describe (".Cursor", function(){

            describe ("#count", function(){

                it ("counts several selected records", function (done) {
                    testCollection.find (
                        { test:'find', able:{ $mod:[ 2, 1 ] }},
                        function (err, cursor) {
                            if (err) return done (err);
                            cursor.count (function (err, count) {
                                if (err) return done (err);
                                if (count !== 4) return done (
                                    new Error ('selected incorrect number of records')
                                );
                                done();
                            });
                        }
                    );
                });

                it ("counts zero selected records", function (done) {
                    testCollection.find (
                        { test:'find', able:{ $mod:[ 2, 1 ] }},
                        function (err, cursor) {
                            if (err) return done (err);
                            cursor.count (function (err, count) {
                                if (err) return done (err);
                                if (count !== 4) return done (
                                    new Error ('selected incorrect number of records')
                                );
                                done();
                            });
                        }
                    );
                });
            });

            describe ("#nextObject", function(){

                it ("retrieves several selected records with nextObject", function (done) {
                    testCollection.find (
                        { test:'find', able:{ $mod:[ 2, 1 ] }},
                        function (err, cursor) {
                            if (err) return done (err);
                            var nextRecord;
                            var ok = { 1:false, 3:false, 5:false, 7:false };
                            async.doWhilst (
                                function (callback) {
                                    nextRecord = undefined;
                                    cursor.nextObject (function (err, item) {
                                        if (err) return callback (err);
                                        if (!item) return callback ();
                                        nextRecord = item;
                                        ok[item.able] = true;
                                        callback();
                                    });
                                },
                                function(){ return Boolean (nextRecord); },
                                function (err) {
                                    if (err) return done (err);
                                    var count = Object.keys (ok).length;
                                    if (count != 4)
                                        return done (new Error (
                                            'retrieved incorrect number of records ('+count+')'
                                        ));
                                    for (var i in ok)
                                        if (!ok[i])
                                            return done (new Error ('retrieved the wrong records'));
                                    done();
                                }
                            );
                        }
                    );
                });
            });

            describe ("#each", function(){

                it ("retrieves several selected records with each", function (done) {
                    testCollection.find (
                        { test:'find', able:{ $mod:[ 2, 1 ] }},
                        function (err, cursor) {
                            if (err) return done (err);
                            var nextRecord;
                            var ok = { 1:false, 3:false, 5:false, 7:false };
                            cursor.each (function (err, item) {
                                if (err) return done (err);
                                if (item) {
                                    nextRecord = item;
                                    ok[item.able] = true;
                                    return;
                                }

                                var count = Object.keys (ok).length;
                                if (count != 4)
                                    return done (new Error (
                                        'retrieved incorrect number of records ('+count+')'
                                    ));
                                for (var i in ok)
                                    if (!ok[i])
                                        return done (new Error ('retrieved the wrong records'));
                                done();
                            });
                        }
                    );
                });
            });

            describe ("#rewind", function(){

                it ("rewinds with #nextObject iteration", function (done) {
                    testCollection.find (
                        { test:'find', able:{ $mod:[ 2, 1 ] }},
                        function (err, cursor) {
                            if (err) return done (err);
                            var nextRecord;
                            var current = 0;
                            var target = 0;
                            var total = 0;
                            async.doWhilst (
                                function (callback) {
                                    cursor.nextObject (function (err, item) {
                                        nextRecord = item;
                                        if (err) return callback (err);
                                        if (!item) return callback ();
                                        total++;

                                        // rewind logic
                                        if (current < target)
                                            current++;
                                        else {
                                            cursor.rewind();
                                            current = 0;
                                            target++;
                                        }

                                        callback();
                                    });
                                },
                                function(){ return Boolean (nextRecord); },
                                function (err) {
                                    if (err) return done (err);
                                    if (total != 14)
                                        return done (new Error ('rewinds did not occur '+total));
                                    done();
                                }
                            );
                        }
                    );
                });

                it ("rewinds with #each iteration", function (done) {
                    testCollection.find (
                        { test:'find', able:{ $mod:[ 2, 1 ] }},
                        function (err, cursor) {
                            if (err) return done (err);
                            var nextRecord;
                            var current = 0;
                            var target = 0;
                            var total = 0;
                            cursor.each (function (err, item) {
                                nextRecord = item;
                                if (err) return done (err);
                                if (!item) {
                                    if (total != 14)
                                        return done (new Error ('rewinds did not occur '+total));
                                    return done();
                                }
                                total++;

                                // rewind logic
                                if (current < target)
                                    current++;
                                else {
                                    cursor.rewind();
                                    current = 0;
                                    target++;
                                }
                            });
                        }
                    );
                });
            });

            describe ("#toArray", function(){

                it ('retrieves several documents as an Array', function (done) {
                    testCollection.find (
                        { test:'find', able:{ $mod:[ 2, 1 ] }},
                        function (err, cursor) {
                            if (err) return done (err);
                            var nextRecord;
                            var current = 0;
                            var target = 0;
                            var total = 0;
                            cursor.toArray (function (err, items) {
                                if (err) return done (err);
                                if (items.length != 4)
                                    return done (new Error (
                                        'retrieved wrong number of documents'
                                    ));

                                var ok = { 1:false, 3:false, 5:false, 7:false };
                                for (var i in items)
                                    ok[items[i].able] = true;
                                if (Object.keys (ok).length != 4)
                                    return done (new Error (
                                        'retrieved wrong number of documents'
                                    ));
                                for (var key in ok)
                                    if (!ok[key])
                                        return done (new Error (
                                            'retrieved wrong document'
                                        ));
                                done();
                            });
                        }
                    );
                });

            });

            describe ("#explain", function(){

                it ("explains a query with converted index names", function (done) {
                    testCollection.ensureIndex ({ xray:-1, zebra:1 }, function (err) {
                        if (err) return done (err);
                        testCollection.find (
                            { xray:2, zebra:2, test:'find' },
                            function (err, cursor) {
                                if (err) return done (err);
                                cursor.explain (function (err, info) {
                                    if (err) return done (err);
                                    if (!info) return done (new Error (
                                        'did not retrieve any query information'
                                    ));
                                    if (info.cursor !== "BtreeCursor xray_-1_zebra_1")
                                        return done (new Error (
                                            'incorrect cursor info "'+info.cursor+'"'
                                        ));
                                    done();
                                });
                            }
                        );
                    });
                });

                it ("handles named indexes without converting", function (done) {
                    testCollection.ensureIndex ({ zulu:1 }, { name:'foogoo' }, function (err) {
                        if (err) return done (err);
                        testCollection.find (
                            { zulu:2, test:'find' },
                            function (err, cursor) {
                                if (err) return done (err);
                                cursor.explain (function (err, info) {
                                    if (err) return done (err);
                                    if (!info) return done (new Error (
                                        'did not retrieve any query information'
                                    ));
                                    if (info.cursor !== "BtreeCursor foogoo")
                                        return done (new Error (
                                            'incorrect cursor info "'+info.cursor+'"'
                                        ));
                                    done();
                                });
                            }
                        );
                    });
                });
            });

            describe ("#close", function(){

                it ("closes the cursor", function (done) {
                    testCollection.find (
                        { test:'find', xrazy:2, zebra:2 },
                        function (err, cursor) {
                            if (err) return done (err);
                            cursor.close (done);
                        }
                    );
                });

            });
        });
    });

    describe ("#findOne", function(){

        it ("retrieves a single document with a simple query", function (done) {
            async.each ([ 0, 1, 2, 3, 4, 5 ], function (i, callback) {
                testCollection.insert (
                    {
                        _id:    getNextID(),
                        able:   i,
                        test:   "simpleFindOne"
                    },
                    { w:1 },
                    callback
                );
            }, function (err) {
                if (err) return done (err);
                testCollection.findOne ({ able:3, test:"simpleFindOne" }, function (err, record) {
                    if (err) return done (err);
                    if (!record)
                        return done (new Error ('failed to retrieve anything'));
                    if (record.able !== 3)
                        return done (new Error ('retrieved the wrong document'));
                    done();
                });
            });
        });

        it ("retrieves a single document with a deep query", function (done) {
            async.each ([ 0, 1, 2, 3, 4, 5 ], function (i, callback) {
                testCollection.insert (
                    {
                        _id:    getNextID(),
                        able:   { baker:{ charlie:{ dog:{ easy:{ fox:i }}}}},
                        test:   "deepFindOne"
                    },
                    { w:1 },
                    callback
                );
            }, function (err) {
                if (err) return done (err);
                testCollection.findOne ({
                    "able.baker.charlie.dog.easy.fox":3, test:"deepFindOne"
                }, function (err, record) {
                    if (err) return done (err);
                    if (!record)
                        return done (new Error ('failed to retrieve anything'));
                    try {
                        if (record.able.baker.charlie.dog.easy.fox !== 3)
                            return done (new Error ('retrieved the wrong document'));
                    } catch (err) {
                        return done (new Error ('retrieved the wrong document'));
                    }
                    done();
                });
            });
        });

        it ("retrieves a single document with a simple Object sort", function (done) {
            async.each ([ 0, 1, 2, 3, 4, 5 ], function (i, callback) {
                testCollection.insert (
                    {
                        _id:    getNextID(),
                        able:   i,
                        test:   "simpleFindOne_sort01"
                    },
                    { w:1 },
                    callback
                );
            }, function (err) {
                if (err) return done (err);
                testCollection.findOne (
                    { able:{ $lt:3 }, test:"simpleFindOne_sort01" },
                    undefined,
                    { sort:{ able:-1 }},
                    function (err, record) {
                        if (err) return done (err);
                        if (!record)
                            return done (new Error ('failed to retrieve anything'));
                        if (record.able !== 2)
                            return done (new Error ('retrieved the wrong document'));
                        done();
                    }
                );
            });
        });

        it ("retrieves a single document with a complex shallow sort", function (done) {
            async.each ([ 0, 1, 2, 3, 4, 5 ], function (i, callback) {
                testCollection.insert (
                    {
                        _id:    getNextID(),
                        zero:   0,
                        able:   i,
                        baker:  5 - i,
                        test:   "simpleFindOne_sort02"
                    },
                    { w:1 },
                    callback
                );
            }, function (err) {
                if (err) return done (err);
                testCollection.findOne (
                    { able:{ $lt:3 }, baker:{ $gt:3 }, test:"simpleFindOne_sort02" },
                    undefined,
                    { sort:[ [ 'zero', 1 ], [ 'able', -1 ], [ 'baker', 1 ] ] },
                    function (err, record) {
                        if (err) return done (err);
                        if (!record)
                            return done (new Error ('failed to retrieve anything'));
                        if (record.able !== 1)
                            return done (new Error ('retrieved the wrong document'));
                        done();
                    }
                );
            });
        });

        it ("retrieves a single document with a complex deep sort", function (done) {
            async.each ([ 0, 1, 2, 3, 4, 5 ], function (i, callback) {
                testCollection.insert (
                    {
                        _id:    getNextID(),
                        able:   {
                            baker:  {
                                zero:   0,
                                able:   i,
                                baker:  5 - i
                            }
                        },
                        test:   "simpleFindOne_sort03"
                    },
                    { w:1 },
                    callback
                );
            }, function (err) {
                if (err) return done (err);
                testCollection.findOne (
                    {
                        'able.baker.able':      { $lt:3 },
                        'able.baker.baker':     { $gt:3 },
                        test:                   "simpleFindOne_sort03"
                    },
                    undefined,
                    { sort:[
                        [ 'able.baker.zero', 1 ],
                        [ 'able.baker.able', -1 ],
                        [ 'able.baker.baker', 1 ]
                    ]},
                    function (err, rec) {
                        if (err) return done (err);
                        if (!rec)
                            return done (new Error ('failed to retrieve anything'));
                        if (rec.able.baker.able !== 1)
                            return done (new Error ('retrieved the wrong document'));
                        done();
                    }
                );
            });
        });
    });


    describe ("#findAll", function(){

        it ("retrieves several documents with a simple query", function (done) {
            async.each ([ 0, 1, 2, 9, 9, 9, 7, 8, 10 ], function (i, callback) {
                testCollection.insert (
                    {
                        _id:    getNextID(),
                        able:   i,
                        test:   "simpleFindAll"
                    },
                    { w:1 },
                    callback
                );
            }, function (err) {
                if (err) return done (err);
                testCollection.findAll ({ able:9, test:"simpleFindAll" }, function (err, records) {
                    if (err) return done (err);
                    if (!records || !records.length)
                        return done (new Error ('failed to retrieve anything'));
                    for (var i in records)
                        if (records[i].able !== 9)
                            return done (new Error ('retrieved the wrong document'));
                    done();
                });
            });
        });

        it ("retrieves several documents with a deep query", function (done) {
            async.each ([ 0, 1, 2, 9, 9, 9, 7, 8, 10 ], function (i, callback) {
                testCollection.insert (
                    {
                        _id:    getNextID(),
                        able:   { baker:{ charlie:{ dog:{ easy:{ fox:i }}}}},
                        test:   "deepFindAll"
                    },
                    { w:1 },
                    callback
                );
            }, function (err) {
                if (err) return done (err);
                testCollection.findAll ({
                    "able.baker.charlie.dog.easy.fox":9, test:"deepFindAll"
                }, function (err, records) {
                    if (err) return done (err);
                    if (!records || !records.length)
                        return done (new Error ('failed to retrieve anything'));
                    try {
                        for (var i in records)
                            if (records[i].able.baker.charlie.dog.easy.fox !== 9)
                                return done (new Error ('retrieved the wrong document'));
                    } catch (err) {
                        return done (new Error ('retrieved the wrong document'));
                    }
                    done();
                });
            });
        });
    });


    describe ("#findAndModify", function(){

        it ("simple query and update, simple Object sort", function (done) {
            async.each ([ 0, 1, 2, 3, 4, 5 ], function (i, callback) {
                testCollection.insert (
                    {
                        _id:    getNextID(),
                        able:   i,
                        test:   "simpleFindAndModify_sort01"
                    },
                    { w:1 },
                    callback
                );
            }, function (err) {
                if (err) return done (err);
                testCollection.findAndModify (
                    { able:{ $lt:3 }, test:"simpleFindAndModify_sort01" },
                    { able:-1 },
                    { $set:{ charlie:9001 }},
                    { new:true },
                    function (err, record) {
                        if (err) return done (err);
                        if (!record)
                            return done (new Error ('failed to retrieve anything'));
                        if (record.able !== 2)
                            return done (new Error ('retrieved the wrong document'));
                        if (record.charlie !== 9001)
                            return done (new Error ('failed to modify the document'));
                        done();
                    }
                );
            });
        });

        it ("simple query and update, complex shallow sort", function (done) {
            async.each ([ 0, 1, 2, 3, 4, 5 ], function (i, callback) {
                testCollection.insert (
                    {
                        _id:    getNextID(),
                        able:   i,
                        baker:  5 - i,
                        test:   "simpleFindAndModify_sort02"
                    },
                    { w:1 },
                    callback
                );
            }, function (err) {
                if (err) return done (err);
                testCollection.findAndModify (
                    { able:{ $lt:3 }, baker:{ $gt:3 }, test:"simpleFindAndModify_sort02" },
                    [ [ 'able', -1 ], [ 'baker', 1 ] ],
                    { $set:{ charlie:9001 }},
                    { new:true },
                    function (err, record) {
                        if (err) return done (err);
                        if (!record)
                            return done (new Error ('failed to retrieve anything'));
                        if (record.able !== 1)
                            return done (new Error ('retrieved the wrong document'));
                        if (record.charlie !== 9001)
                            return done (new Error ('failed to modify the document'));
                        done();
                    }
                );
            });
        });

        it ("deep query and update, complex deep sort", function (done) {
            async.each ([ 0, 1, 2, 3, 4, 5 ], function (i, callback) {
                testCollection.insert (
                    {
                        _id:    getNextID(),
                        able:   {
                            able:   i,
                            baker:  5 - i
                        },
                        test:   "simpleFindAndModify_sort03"
                    },
                    { w:1 },
                    callback
                );
            }, function (err) {
                if (err) return done (err);
                testCollection.findAndModify (
                    {
                        "able.able":    { $lt:3 },
                        "able.baker":   { $gt:3 },
                        test:           "simpleFindAndModify_sort03"
                    },
                    [ [ 'able.able', -1 ], [ 'able.baker', 1 ] ],
                    { $set:{ charlie:9001 }},
                    { new:true },
                    function (err, record) {
                        if (err) return done (err);
                        if (!record)
                            return done (new Error ('failed to retrieve anything'));
                        try {
                            if (record.able.able !== 1)
                                return done (new Error ('retrieved the wrong document'));
                        } catch (err) {
                            return done (new Error ('retrieved the wrong document'));
                        }
                        if (record.charlie !== 9001)
                            return done (new Error ('failed to modify the document'));
                        done();
                    }
                );
            });
        });
    });


    describe ("#findAndRemove", function(){

        it ("finds and removes a document", function (done) {
            testCollection.insert (
                { test:'findAndRemove', charlie:{ baker:{ able:42 }}},
                { w:1 },
                function (err) {
                    if (err) return done (err);
                    testCollection.findAndRemove ({ 'charlie.baker.able':42 }, function (err, rec) {
                        if (err) return done (err);
                        if (!rec)
                            return done (new Error ('did not retrieve the document'));
                        testCollection.find ({ test:'findAndRemove' }, function (err, cursor) {
                            if (err) return done (err);
                            cursor.count (function (err, n) {
                                if (err) return done (err);
                                if (n)
                                    return done (new Error ('did not delete the document'));
                                done();
                            });
                        });
                    });
                }
            );
        });

    });

    describe ("#update", function(){

        before (function (done) {
            testCollection.insert (
                {
                    test:       'updates',
                    able:       97,
                    baker:      {
                        able:       {
                            charlie:    103
                        }
                    },
                    charlie:    [
                        {
                            able:       0,
                            baker:      9000
                        },
                        {
                            able:       1,
                            baker:      9000
                        },
                        {
                            able:       2,
                            baker:      9000
                        },
                        {
                            able:       3,
                            baker:      9000
                        },
                        {
                            able:       4,
                            baker:      9000
                        }
                    ]
                },
                { w:1 },
                done
            );
        });

        it ("updates a document", function (done) {
            testCollection.update (
                { test:'updates', 'baker.able.charlie':{ $gt:10 }},
                { $inc:{ able:3, 'baker.able.charlie':97 }},
                { w:1 },
                function (err) {
                    if (err) return done (err);
                    testCollection.findOne ({ test:'updates' }, function (err, rec) {
                        if (err) return done (err);
                        if (!rec)
                            return done (new Error ('failed to retrieve document after update'));

                        delete rec._id;

                        if (!matchLeaves (rec, {
                                test:       'updates',
                                able:       100,
                                baker:      {
                                    able:       {
                                        charlie:    200
                                    }
                                },
                                charlie:    [
                                    {
                                        able:       0,
                                        baker:      9000
                                    },
                                    {
                                        able:       1,
                                        baker:      9000
                                    },
                                    {
                                        able:       2,
                                        baker:      9000
                                    },
                                    {
                                        able:       3,
                                        baker:      9000
                                    },
                                    {
                                        able:       4,
                                        baker:      9000
                                    }
                                ]
                        }))
                            return done (new Error ('record did not match expected outcome'));
                        done();
                    });
                }
            );
        });

        it ("updates a document with the positional operator", function (done) {
            testCollection.update (
                { test:'updates', 'charlie.able':{ $gt:2 }},
                { $inc:{ 'charlie.$.baker':1 }},
                { w:1 },
                function (err) {
                    if (err) return done (err);
                    testCollection.findOne ({ test:'updates' }, function (err, rec) {
                        if (err) return done (err);
                        if (!rec)
                            return done (new Error ('failed to retrieve document after update'));

                        delete rec._id;

                        assert.deepEqual (
                            rec,
                            {
                                test:       'updates',
                                able:       100,
                                baker:      {
                                    able:       {
                                        charlie:    200
                                    }
                                },
                                charlie:    [
                                    {
                                        able:       0,
                                        baker:      9000
                                    },
                                    {
                                        able:       1,
                                        baker:      9000
                                    },
                                    {
                                        able:       2,
                                        baker:      9000
                                    },
                                    {
                                        able:       3,
                                        baker:      9001
                                    },
                                    {
                                        able:       4,
                                        baker:      9000
                                    }
                                ]
                            },
                            'record did not match expected outcome'
                        );
                        done();
                    });
                }
            );
        });

    });


    describe ("#remove", function(){

        it ("removes several documents", function (done) {
            async.times (8, function (n, callback) {
                testCollection.insert ({
                    _id:        getNextID(),
                    test:       'remove',
                    removalKey: n
                }, { w:1 }, callback);
            }, function (err) {
                if (err) return done (err);
                testCollection.remove (
                    { removalKey:{ $gte:2, $lte:4 } },
                    { w:1, fsync:true },
                    function (err) {
                        if (err) return done (err);
                        testCollection.find ({ test:'remove' }, function (err, cursor) {
                            if (err) return done (err);
                            cursor.toArray (function (err, recs) {
                                if (err) return done (err);
                                if (!recs)
                                    return done (new Error ('removed everything!'));
                                if (recs.length != 5)
                                    return done (new Error ('removed wrong number of documents'));

                                for (var i in recs) {
                                    var rec = recs[i];
                                    if (
                                        !Object.hasOwnProperty.call (rec, 'removalKey')
                                     || ( rec.removalKey >= 2 && rec.removalKey <= 4 )
                                    )
                                        return done (new Error ('removed wrong documents'));
                                }

                                done()
                            });
                        });
                    }
                );
            });
        });

    });


    describe ("#distinct", function(){

        before (function (done) {
            async.times (6, function (n, callback) {
                testCollection.insert ({
                    _id:            getNextID(),
                    test:           'distinct',
                    distinctive:    n
                }, { w:1 }, callback);
            }, done);
        });

        it ('retrieves several distinct values with a simple key', function (done) {
            testCollection.distinct ('distinctive', function (err, vals) {
                if (err) return done (err);
                if (vals.length != 6) return done (new Error (
                    'retrieved wrong number of distinct values'
                ));
                var results = { 0:false, 1:false, 2:false, 3:false, 4:false, 5:false };
                for (var i in vals)
                    results[vals[i]] = true;
                if (Object.keys (results).length != 6) return done (new Error (
                    'retrieved wrong values'
                ));
                for (var key in results)
                    if (!results[key]) return done (new Error (
                        'retrieved wrong values'
                    ));
                done();
            });
        });

        it ('retrieves several distinct values with a limiting query', function (done) {
            testCollection.distinct ('distinctive', { distinctive:{ $lt:4 } }, function (err, vals) {
                if (err) return done (err);
                if (vals.length != 4) return done (new Error (
                    'retrieved wrong number of distinct values'
                ));
                var results = { 0:false, 1:false, 2:false, 3:false };
                for (var i in vals)
                    results[vals[i]] = true;
                if (Object.keys (results).length != 4) return done (new Error (
                    'retrieved wrong values'
                ));
                for (var key in results)
                    if (!results[key]) return done (new Error (
                        'retrieved wrong values'
                    ));
                done();
            });
        });

        it ('retrieves several distinct array subdocuments with a limiting query', function (done) {
            var results = [];
            async.each ([ testCollection, rawTestCollection ], function (collection, callback) {
                async.each ([ 0, 1, 2, 3, 4, 5 ], function (i, callback) {
                    collection.insert (
                        {
                            _id:    getNextID(),
                            able:   [
                                {
                                    able:   i,
                                    baker:  5 - i
                                },
                                {
                                    able:   i + 10,
                                    baker:  15 - i
                                },
                                {
                                    able:   i + 20,
                                    baker:  25 - i
                                }
                            ],
                            test:   "distinct_limit_01"
                        },
                        { w:1 },
                        callback
                    );
                }, function (err) {
                    if (err) return done (err);

                    collection.distinct ('able', { 'able.able':{ $gte:20 }}, function (err, vals) {
                        if (err) return done (err);
                        results.push (vals);
                        callback();
                    });
                });
            }, function (err) {
                if (err) return done (err);
                var zeroth = results[0];
                for (var i=1,j=results.length; i<j; i++) {
                    if (zeroth.length != results[i].length)
                        return done (new Error ('retrieved wrong number of results'));
                    for (var k in zeroth) {
                        var found = false;
                        for (var l in results[i])
                            if (matchLeaves (zeroth[k], results[i][l])) {
                                found = true;
                                break;
                            }
                        if (!found)
                            return done (new Error ('retrieved incorrect result set'));
                    }
                }
                done();
            });

        });

        it ('retrieves several distinct array subdocuments on a deep path (with query)', function (done) {
            var results = [];
            async.each ([ testCollection, rawTestCollection ], function (collection, callback) {
                async.each ([ 0, 1, 2, 3, 4, 5 ], function (i, callback) {
                    collection.insert (
                        {
                            _id:    getNextID(),
                            able:   {
                                able:   {
                                    able:   [
                                        {
                                            able:   i,
                                            baker:  5 - i
                                        },
                                        {
                                            able:   i + 10,
                                            baker:  15 - i
                                        },
                                        {
                                            able:   i + 20,
                                            baker:  25 - i
                                        }
                                    ],
                                }
                            },
                            test:   "distinct_limit_02"
                        },
                        { w:1 },
                        callback
                    );
                }, function (err) {
                    if (err) return done (err);

                    collection.distinct (
                        'able.able.able',
                        { 'able.able.able.able':{ $gte:20 }},
                        function (err, vals) {
                            if (err) return done (err);
                            results.push (vals);
                            callback();
                        }
                    );
                });
            }, function (err) {
                if (err) return done (err);
                var zeroth = results[0];
                for (var i=1,j=results.length; i<j; i++) {
                    if (zeroth.length != results[i].length)
                        return done (new Error ('retrieved wrong number of results'));
                    for (var k in zeroth) {
                        var found = false;
                        for (var l in results[i])
                            if (matchLeaves (zeroth[k], results[i][l])) {
                                found = true;
                                break;
                            }
                        if (!found)
                            return done (new Error ('retrieved incorrect result set'));
                    }
                }
                done();
            });
        });

    });

    describe ("#count", function(){

        it ("counts records matching a query", function (done) {
            async.each ([ 0, 1, 2, 3, 4, 5 ], function (i, callback) {
                testCollection.insert (
                    {
                        _id:    getNextID(),
                        able:   i,
                        test:   "simpleFindOne"
                    },
                    { w:1 },
                    callback
                );
            }, function (err) {
                if (err) return done (err);
                testCollection.findOne ({ able:3, test:"simpleFindOne" }, function (err, record) {
                    if (err) return done (err);
                    if (!record)
                        return done (new Error ('failed to retrieve anything'));
                    if (record.able !== 3)
                        return done (new Error ('retrieved the wrong document'));
                    done();
                });
            });
        });

    });

    describe ("#options", function(){

        it ("always returns an explanatory Error", function (done) {
            var sync = true;
            testCollection.options (function (err, options) {
                if (sync)
                    return done (new Error (
                        'callback fired synchronously'
                    ));
                if (options)
                    return done (new Error (
                        'returned an options object'
                    ));
                if (!err)
                    return done (new Error (
                        'did not return an Error'
                    ));
                if (err.message != 'the Node.js MongoDB driver does not support #options')
                    return done (new Error (
                        'did not return the correct Error'
                    ));

                done();
            });
            sync = false;
        });

    });

    describe ("uncompressed subdocuments", function(){

        it ("deactivates compression for a path", function (done) {
            testCollection.setDecompressed ('big', function (err) {
                if (err) return done (err);
                var document = {
                    _id:    getNextID(),
                    test:   'uncompressed',
                    big:    {
                        fat:    {
                            this:       42,
                            that:       'forty-two',
                            another:    'quatorze deuce or something'
                        }
                    }
                };
                testCollection.insert (document, { w:1 }, function (err) {
                    if (err) return done (err);
                    testCollection.findOne ({ test:'uncompressed' }, function (err, rec) {
                        if (err) return done (err);
                        if (!rec) return done (new Error ('failed to retrieve test record'));
                        if (!matchLeaves (rec, document))
                            return done (new Error ('retrieved test document did not match'));
                        rawTestCollection.findOne ({ _id:document._id }, function (err, rec) {
                            if (err) return done (err);
                            if (!rec) return done (new Error (
                                'raw collection cannot confirm test record'
                            ));
                            for (var key in rec)
                                if (matchLeaves (rec[key], {
                                        fat:    {
                                            this:       42,
                                            that:       'forty-two',
                                            another:    'quatorze deuce or something'
                                        }
                                    }
                                ))
                                    return done();

                            return done (new Error ('subdocument was damaged or compressed'));
                        });
                    });
                });
            });
        });

        it ("reactivates compression for a path");

    });

    describe ("namespace aliases", function(){

        it ("creates an alias from one path to another");

        it ("creates an alias to another namespace root");

        it ("creates an alias to a path on another namespace");

    });

    describe ("#geoNear", function(){

        it ("finds documents located near a legacy point");

        it ("finds documents located near a `2dsphere` point");

    });

    describe ("#geoHaystack", function(){



    });

    describe ("#stats", function(){



    });

    describe ("#initializeOrderedBulkOp", function(){



    });

    describe ("#parallelCollectionScan", function(){



    });
});
