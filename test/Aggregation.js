
var async = require ('async');
var mingydb = require ('../main');
var fauxmongo = require ('fauxmongo');

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
var collection, rawCollection, targetCollection;
function testAggregation (documents, pipeline, callback, arraysAsSets) {
    async.parallel ([
        function (callback) {
            collection.remove ({}, { w:1, fsync:true }, callback);
        },
        function (callback) {
            targetCollection.remove ({}, { w:1, fsync:true }, callback);
        }
    ], function (err) {
        if (err) return callback (err);
        async.parallel ([
            function (callback) {
                async.each (documents, function (doc, callback) {
                    collection.insert (doc, { w:1 }, callback);
                }, callback);
            },
            function (callback) {
                async.each (documents, function (doc, callback) {
                    targetCollection.insert (doc, { w:1 }, callback);
                }, callback);
            }
        ], function (err) {
            if (err) return callback (err);

            // process the same operation raw and compressed, then compare results
            var sample, target;
            async.parallel ([
                function (callback) {
                    var pipeCheck = JSON.stringify (pipeline);
                    collection.aggregate (pipeline, function (err, recs) {
                        if (err) return callback (err);

                        if (JSON.stringify (pipeline) != pipeCheck)
                            return callback (new Error (
                                'compression damaged input pipeline spec'
                            ));
                        sample = recs;
                        callback();
                    });
                },
                function (callback) {
                    targetCollection.aggregate (pipeline, function (err, recs) {
                        if (err) return callback (err);
                        target = recs;
                        callback();
                    });
                },
            ], function (err) {
                if (err) return callback (err);

                for (var i in target) delete target[i]._id;
                for (var i in sample) delete sample[i]._id;

                if (sample.length != target.length) {
                    console.log (JSON.stringify (target));
                    console.log (JSON.stringify (sample));
                    return callback (new Error (
                        'mingydb produced incorrect number of records'
                    ));
                }

                for (var i=0,j=sample.length; i<j; i++) {
                    var candidate = sample[i];
                    var found = false;
                    for (var k in target)
                        if (matchLeaves (candidate, target[k], true)) {
                            found = true;
                            break;
                        }
                    if (!found) {
                        console.log (
                            JSON.stringify (sample)
                           .replace ('[', '[\n  ')
                           .replace (',', ',\n  ')
                           .replace (']', ']\n')
                        );
                        console.log (
                            JSON.stringify (target)
                           .replace ('[', '[\n  ')
                           .replace (',', ',\n  ')
                           .replace (']', ']\n')
                        );
                        return callback (new Error ('mingydb and mongodb disagreed'));
                    }
                }

                callback();
            });
        });
    });
}

describe ("Aggregation", function(){

    this.slow (500);

    before (function (done) {
        mingydb.collection (
            'test-mingydb',
            'test-mingydb',
            new mingydb.Server ('127.0.0.1', 27017),
            function (err, col) {
                if (err) return done (err);
                collection = col;
                mingydb.rawCollection (
                    'test-mingydb',
                    'test-mingydb',
                    new mingydb.Server ('127.0.0.1', 27017),
                    function (err, col) {
                        if (err) return done (err);
                        rawCollection = col;
                        mingydb.rawCollection (
                            'test-mingydb',
                            'test-mingydb-raw',
                            new mingydb.Server ('127.0.0.1', 27017),
                            function (err, col) {
                                if (err) return done (err);
                                targetCollection = col;
                                collection.remove ({}, { w:1, j:1 }, function (err) {
                                    if (err) return callback (err);
                                    targetCollection.remove ({}, { w:1, j:1 }, function (err) {
                                        if (err) return callback (err);
                                        done();
                                    });
                                });
                            }
                        );
                    }
                );
            }
        );
    });

    describe ("#aggregate", function(){

        // describe ("$match", function(){

        //     it ("selectively passes documents with a query", function (done) {
        //         testAggregation ([
        //             { able:9, baker:1 },
        //             { able:8, baker:2 },
        //             { able:7, baker:3 },
        //             { able:6, baker:4 },
        //             { able:5, baker:5 },
        //             { able:4, baker:6 },
        //             { able:3, baker:7 },
        //             { able:2, baker:8 },
        //             { able:1, baker:9 }
        //         ], [
        //             {
        //                 $match:     {
        //                     able:   { $gt:2 },
        //                     baker:  { $gt:2 }
        //                 }
        //             }
        //         ], done);
        //     });

        // });

        describe ("$project", function(){

            it ("projects records with dot notation", function (done) {
                testAggregation ([
                    { able:{ baker:9 }, baker:42 },
                    { able:{ baker:8 }, baker:42 },
                    { able:{ baker:7 }, baker:42, charlie:9 },
                    { able:{ baker:6 }, baker:42 },
                ], [
                    { $project: {
                        'able.baker':   1,
                        charlie:        true
                    }}
                ], done);
            });

            it ("projects records with layered specifications", function (done) {
                testAggregation ([
                    { able:{ baker:9 }, baker:42 },
                    { able:{ baker:8 }, baker:42 },
                    { able:{ baker:7 }, baker:42, charlie:9 },
                    { able:{ baker:6 }, baker:42 },
                ], [
                    { $project: {
                        able:       {
                            baker:      1
                        },
                        charlie:    true
                    }}
                ], done);
            });

            it ("projects records with novel dotted fields", function (done) {
                testAggregation ([
                    { able:{ baker:9 }, baker:42 },
                    { able:{ baker:8 }, baker:42 },
                    { able:{ baker:7 }, baker:42, charlie:9 },
                    { able:{ baker:6 }, baker:42 },
                ], [
                    { $project: {
                        'novel.field.addition.here':  '$able.baker'
                    }}
                ], done);
            });

            it ("projects records with novel layered fields", function (done) {
                testAggregation ([
                    { able:{ baker:9 }, baker:42 },
                    { able:{ baker:8 }, baker:42 },
                    { able:{ baker:7 }, baker:42, charlie:9 },
                    { able:{ baker:6 }, baker:42 },
                ], [
                    { $project: {
                        novel:      {
                            field:      {
                                addition:   {
                                    here:       '$able.baker'
                                }
                            }
                        }
                    }}
                ], done);
            });

        });

        describe ("$sort", function(){

            it ("sorts the input records", function (done) {
                testAggregation ([
                    { able:{ baker:9 }, baker:42 },
                    { able:{ baker:8 }, baker:42 },
                    { able:{ baker:7 }, baker:42, charlie:9 },
                    { able:{ baker:6 }, baker:42 },
                ], [
                    { $sort:{
                        'able.baker':   1,
                        baker:          -1
                    }}
                ], done);
            });

        });

        describe ("$skip", function(){

            it ("skips documents", function (done) {
                testAggregation ([
                    { able:{ baker:9 }, baker:42 },
                    { able:{ baker:8 }, baker:42 },
                    { able:{ baker:7 }, baker:42, charlie:9 },
                    { able:{ baker:6 }, baker:42 },
                ], [
                    { $sort:{ 'able.baker':1 }},
                    { $skip:2 }
                ], done);
            });

        });

        describe ("$unwind", function(){

            it ("unwinds documents by arrays of subdocuments", function (done) {
                testAggregation ([
                    { able:{ baker:[ 0, 1, 2 ], charlie:9 }, foo:'ber' },
                    { able:{ baker:[ 3, 4, 5 ], charlie:'niner' }, foo:'ma' },
                    { able:{ baker:[ 6, 7, 8 ], charlie:'chaplin' }, foo:'shave' }
                ], [
                    { $unwind:'$able.baker' }
                ], done);
            });

        });

        describe ("$limit", function(){

            it ("limits the number of input documents", function (done) {
                testAggregation ([
                    { able:{ baker:9 }, baker:42 },
                    { able:{ baker:8 }, baker:42 },
                    { able:{ baker:7 }, baker:42, charlie:9 },
                    { able:{ baker:6 }, baker:42 },
                ], [
                    { $sort:{ 'able.baker':1 }},
                    { $limit:2 }
                ], done);
            });

        });

        describe ("$group", function(){

            it ("performs the $group stage operation", function (done) {
                testAggregation ([
                    { able:{ baker:9 },         foo:4 },
                    { able:{ baker:'niner' },   foo:1 },
                    { able:{ baker:9 },         foo:2 },
                    { able:{ baker:'niner' },   foo:1 },
                    { able:{ baker:{ able:2 }}, foo:1 },
                    { able:{ baker:8 },         foo:8 },
                    { able:{ baker:7 },         foo:5 },
                    { able:{ baker:'niner' },   foo:1 },
                    { able:{ baker:9 },         foo:1 },
                    { able:{ baker:{ able:1 }}, foo:3 },
                    { able:{ baker:9 },         foo:9 },
                    { able:{ baker:{ able:1 }}, foo:700 },
                    { able:{ baker:7 },         foo:3 },
                    { able:{ baker:9 },         foo:56 }
                ], [
                    { $group:{
                        _id:        '$able.baker',
                        totalFoo:   { $sum:'$foo' }
                    }}
                ], done);
            });

            describe ("accumulators", function(){

                describe ("$addToSet", function(){

                    it ("accumulates a set of unique items", function (done) {
                        testAggregation ([
                            { able:{ baker:9 },         foo:4 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:9 },         foo:2 },
                            { able:{ baker:'niner' },   foo:'one' },
                            { able:{ baker:{ able:2 }}, foo:'one' },
                            { able:{ baker:8 },         foo:8 },
                            { able:{ baker:7 },         foo:5 },
                            { able:{ baker:'niner' },   foo:'one' },
                            { able:{ baker:9 },         foo:1 },
                            { able:{ baker:{ able:1 }}, foo:3 },
                            { able:{ baker:{ able:1 }}, foo:'two' },
                            { able:{ baker:{ able:1 }}, foo:700 },
                            { able:{ baker:9 },         foo:56 }
                        ], [
                            { $group:{
                                _id:        '$able.baker',
                                fooSet:     { $addToSet:'$foo' }
                            }}
                        ], done, true);
                    });

                });

                describe ("$avg", function(){

                    it ("accumulates Numbers", function (done) {
                        testAggregation ([
                            { able:{ baker:9 },         foo:4 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:9 },         foo:2 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:{ able:2 }}, foo:1 },
                            { able:{ baker:8 },         foo:8 },
                            { able:{ baker:7 },         foo:5 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:9 },         foo:1 },
                            { able:{ baker:{ able:1 }}, foo:3 },
                            { able:{ baker:9 },         foo:9 },
                            { able:{ baker:{ able:1 }}, foo:700 },
                            { able:{ baker:7 },         foo:3 },
                            { able:{ baker:9 },         foo:56 }
                        ], [
                            { $group:{
                                _id:        '$able.baker',
                                averageFoo: { $avg:'$foo' }
                            }}
                        ], done);
                    });

                });

                describe ("$first", function(){

                    it ("selects the first document", function (done) {
                        testAggregation ([
                            { able:{ baker:9 },         foo:4 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:9 },         foo:2 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:{ able:2 }}, foo:1 },
                            { able:{ baker:8 },         foo:8 },
                            { able:{ baker:7 },         foo:5 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:9 },         foo:1 },
                            { able:{ baker:{ able:1 }}, foo:3 },
                            { able:{ baker:9 },         foo:9 },
                            { able:{ baker:{ able:1 }}, foo:700 },
                            { able:{ baker:7 },         foo:3 },
                            { able:{ baker:9 },         foo:56 }
                        ], [
                            { $sort:{
                                'able.baker':   1,
                                foo:            -1
                            }},
                            { $group:{
                                _id:        '$able.baker',
                                firstFoo:   { $first:'$foo' }
                            }}
                        ], done);
                    });

                });

                describe ("$last", function(){

                    it ("selects the last document", function (done) {
                        testAggregation ([
                            { able:{ baker:9 },         foo:4 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:9 },         foo:2 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:{ able:2 }}, foo:1 },
                            { able:{ baker:8 },         foo:8 },
                            { able:{ baker:7 },         foo:5 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:9 },         foo:1 },
                            { able:{ baker:{ able:1 }}, foo:3 },
                            { able:{ baker:9 },         foo:9 },
                            { able:{ baker:{ able:1 }}, foo:700 },
                            { able:{ baker:7 },         foo:3 },
                            { able:{ baker:9 },         foo:56 }
                        ], [
                            { $sort:{
                                'able.baker':   1,
                                foo:            -1
                            }},
                            { $group:{
                                _id:        '$able.baker',
                                lastFoo:    { $last:'$foo' }
                            }}
                        ], done);
                    });

                });

                describe ("$max", function(){

                    it ("selects the highest value", function (done) {
                        testAggregation ([
                            { able:{ baker:9 },         foo:4 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:9 },         foo:2 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:{ able:2 }}, foo:1 },
                            { able:{ baker:8 },         foo:8 },
                            { able:{ baker:7 },         foo:5 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:9 },         foo:1 },
                            { able:{ baker:{ able:1 }}, foo:3 },
                            { able:{ baker:9 },         foo:9 },
                            { able:{ baker:{ able:1 }}, foo:700 },
                            { able:{ baker:7 },         foo:3 },
                            { able:{ baker:9 },         foo:56 }
                        ], [
                            { $group:{
                                _id:        '$able.baker',
                                biggestFoo: { $max:'$foo' }
                            }}
                        ], done);
                    });

                });

                describe ("$min", function(){

                    it ("selects the highest value", function (done) {
                        testAggregation ([
                            { able:{ baker:9 },         foo:4 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:9 },         foo:2 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:{ able:2 }}, foo:1 },
                            { able:{ baker:8 },         foo:8 },
                            { able:{ baker:7 },         foo:5 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:9 },         foo:1 },
                            { able:{ baker:{ able:1 }}, foo:3 },
                            { able:{ baker:9 },         foo:9 },
                            { able:{ baker:{ able:1 }}, foo:700 },
                            { able:{ baker:7 },         foo:3 },
                            { able:{ baker:9 },         foo:56 }
                        ], [
                            { $group:{
                                _id:        '$able.baker',
                                lilestFoo:  { $min:'$foo' }
                            }}
                        ], done);
                    });

                });

                describe ("$push", function(){

                    it ("creates arrays of values", function (done) {
                        testAggregation ([
                            { able:{ baker:9 },         foo:4 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:9 },         foo:2 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:8 },         foo:8 },
                            { able:{ baker:7 },         foo:5 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:9 },         foo:1 },
                            { able:{ baker:9 },         foo:9 },
                            { able:{ baker:7 },         foo:3 },
                            { able:{ baker:9 },         foo:56 }
                        ], [
                            { $group:{
                                _id:        '$able.baker',
                                fooRoster:  { $push:{ foo:'$foo', fooParentID:'$able.baker' } }
                            }}
                        ], done);
                    });

                });

                describe ("$sum", function(){

                    it ("accumulates Numbers", function (done) {
                        testAggregation ([
                            { able:{ baker:9 },         foo:4 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:9 },         foo:2 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:{ able:2 }}, foo:1 },
                            { able:{ baker:8 },         foo:8 },
                            { able:{ baker:7 },         foo:5 },
                            { able:{ baker:'niner' },   foo:1 },
                            { able:{ baker:9 },         foo:1 },
                            { able:{ baker:{ able:1 }}, foo:3 },
                            { able:{ baker:9 },         foo:9 },
                            { able:{ baker:{ able:1 }}, foo:700 },
                            { able:{ baker:7 },         foo:3 },
                            { able:{ baker:9 },         foo:56 }
                        ], [
                            { $group:{
                                _id:        '$able.baker',
                                sumFoo:     { $sum:'$foo' }
                            }}
                        ], done);
                    });

                });

            });

        });

        describe ("$redact", function(){

            it ("redacts entire documents", function (done) {
                testAggregation ([
                    { able:{ baker:[ 'a', 'b' ] } },
                    { able:{ baker:[ 'b', 'c' ] } },
                    { able:{ baker:[ 'c', 'd' ] } },
                    { able:{ baker:[ 'd', 'b' ] } },
                ], [
                    { $redact:{
                        $cond:{
                            if:         { $gt: [ { $size:{ $setIntersection:[
                                "$able.baker",
                                [ 'b' ]
                            ] } }, 0 ] },
                            then:       '$$KEEP',
                            'else':     '$$PRUNE'
                        }
                    } }
                ], done);
            });

        });

        describe ("$geoNear", function(){

            it ("selects documents geolocated near a point", function (done) {
                var documents = [
                    { xray:{ zebra:[ 1, 20 ] }, zebra:{ xray:1 } },
                    { xray:{ zebra:[ -7, 11 ] }, zebra:{ xray:1 } },
                    { xray:{ zebra:[ 15, -4 ] }, zebra:{ xray:0 } },
                    { xray:{ zebra:[ 14, -20 ] }, zebra:{ xray:0 } },
                    { xray:{ zebra:[ 0, -10 ] }, zebra:{ xray:1 } },
                    { xray:{ zebra:[ -1, 7 ] }, zebra:{ xray:0 } },
                    { xray:{ zebra:[ 4, 9 ] }, zebra:{ xray:1 } },
                    { xray:{ zebra:[ -16, 13 ] }, zebra:{ xray:0 } },
                    { xray:{ zebra:[ -12, -13 ] }, zebra:{ xray:1 } },
                    { xray:{ zebra:[ 2, -2 ] }, zebra:{ xray:1 } }
                ];
                var pipeline = [
                    { $geoNear:{
                        near:           [ 0, 0 ],
                        distanceField:  "projected.distance",
                        includeLocs:    "projected.location",
                        num:            2,
                        query:          { 'zebra.xray':{ $gt:0 } }
                    } }
                ];

                async.parallel ([
                    function (callback) {
                        collection.remove ({}, { w:1, fsync:true }, function (err) {
                            if (err) return callback (err);
                            collection.ensureIndex (
                                { 'xray.zebra':'2d' },
                                { w:1, fsync:true },
                                function (err) {
                                    if (err) return callback (err);
                                    async.each (documents, function (doc, callback) {
                                        collection.insert (doc, { w:1, fsync:1 }, callback);
                                    }, callback);
                                }
                            );
                        });
                    },
                    function (callback) {
                        targetCollection.remove ({}, { w:1, fsync:true }, function (err) {
                            if (err) return callback (err);
                            targetCollection.ensureIndex (
                                { 'xray.zebra':'2d' },
                                { w:1, fsync:true },
                                function (err) {
                                    if (err) return callback (err);
                                    async.each (documents, function (doc, callback) {
                                        targetCollection.insert (doc, { w:1, fsync:true }, callback);
                                    }, callback);
                                }
                            );
                        });
                    }
                ], function (err) {
                    if (err) return done (err);

                    // process the same operation raw and compressed, then compare results
                    var sample, target;
                    async.parallel ([
                        function (callback) {
                            var pipeCheck = JSON.stringify (pipeline);
                            collection.aggregate (pipeline, function (err, recs) {
                                if (err) return callback (err);
                                if (JSON.stringify (pipeline) != pipeCheck)
                                    return callback (new Error (
                                        'compression damaged input pipeline spec'
                                    ));
                                sample = recs;
                                callback();
                            });
                        },
                        function (callback) {
                            targetCollection.aggregate (pipeline, function (err, recs) {
                                if (err) return callback (err);
                                target = recs;
                                callback();
                            });
                        },
                    ], function (err) {
                        if (err) return done (err);

                        for (var i in target) delete target[i]._id;
                        for (var i in sample) delete sample[i]._id;

                        if (sample.length != target.length) {
                            console.log (JSON.stringify (target));
                            console.log (JSON.stringify (sample));
                            return done (new Error (
                                'mingydb returned the wrong number of records'
                            ));
                        }

                        for (var i=0,j=sample.length; i<j; i++) {
                            var candidate = sample[i];
                            var found = false;
                            for (var k in target)
                                if (matchLeaves (candidate, target[k], true)) {
                                    found = true;
                                    break;
                                }
                            if (!found) {
                                console.log (JSON.stringify (target));
                                console.log (JSON.stringify (sample));
                                return done (new Error ('mingydb and mongodb disagreed'));
                            }
                        }

                        done();
                    });
                });
            });

        });

        describe ("$out", function(){

            it ("outputs to another compressed collection with $group", function (done) {
                var testDoc;
                collection.insert (
                    testDoc = {
                        test:   'namespace/group',
                        fox:    {
                            george: {
                                hotel:  9001
                            },
                            hotel:  9002
                        },
                        george: {
                            hotel:  9003
                        },
                        hotel:  9004
                    },
                    { w:1 },
                    function (err) {
                        if (err) return done (err);
                        collection.aggregate ([
                            { $match:{ test:'namespace/group' } },
                            { $group:{
                                _id:        '$hotel',
                                test:       { $first:'$test' },
                                foo:        { $sum:'$fox.george.hotel' },
                                bar:        { $sum:'$george.hotel' }
                            } },
                            { $out:'test-mingydb-aggregation' }
                        ], function (err, recs) {
                            if (err) return done (err);
                            mingydb.collection (
                                'test-mingydb',
                                'test-mingydb-aggregation',
                                new mingydb.Server ('127.0.0.1', 27017),
                                function (err, outputCollection) {
                                    if (err) return done (err);
                                    outputCollection.findOne (
                                        { test:'namespace/group'},
                                        function (err, rec) {
                                            if (err) return done (err);
                                            if (!rec) return done (new Error (
                                                'unable to retrieve output record'
                                            ));

                                            if (!matchLeaves (rec, {
                                                _id:    9004,
                                                test:   'namespace/group',
                                                foo:    9001,
                                                bar:    9003
                                            })) {
                                                console.log (rec);
                                                return done (new Error ('retrieved document was incorrect'));
                                            }

                                            done();
                                        }
                                    );
                                }
                            );
                        });
                    }
                );
            });

            it ("outputs to another compressed collection with $project", function (done) {
                var testDoc;
                collection.insert (
                    testDoc = {
                        test:   'namespace/project',
                        fox:    {
                            george: {
                                hotel:  9001
                            },
                            hotel:  9002
                        },
                        george: {
                            hotel:  9003
                        },
                        hotel:  9004
                    },
                    { w:1 },
                    function (err) {
                        if (err) return done (err);
                        collection.aggregate ([
                            { $match:{ test:'namespace/project' } },
                            { $project:{
                                _id:                '$hotel',
                                test:               '$test',
                                'fox.george.hotel': '$fox.george.hotel',
                                bar:                '$george.hotel'
                            } },
                            { $out:'test-mingydb-aggregation' }
                        ], function (err, recs) {
                            if (err) return done (err);
                            mingydb.collection (
                                'test-mingydb',
                                'test-mingydb-aggregation',
                                new mingydb.Server ('127.0.0.1', 27017),
                                function (err, outputCollection) {
                                    if (err) return done (err);
                                    outputCollection.findOne (
                                        { test:'namespace/project'},
                                        function (err, rec) {
                                            if (err) return done (err);
                                            if (!rec) return done (new Error (
                                                'unable to retrieve output record'
                                            ));

                                            if (!matchLeaves (rec, {
                                                _id:    9004,
                                                test:   'namespace/project',
                                                fox:{ george:{ hotel:9001 } },
                                                bar:    9003
                                            })) {
                                                return done (new Error ('retrieved document was incorrect'));
                                            }

                                            done();
                                        }
                                    );
                                }
                            );
                        });
                    }
                );
            });

        });

    });

});
