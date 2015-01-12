
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
var collection, targetCollection;
function testAggregation (documents, pipeline, callback, arraysAsSets) {
    collection.remove ({}, { w:1, j:true }, function (err) {
        if (err) return callback (err);
        async.each (documents, function (doc, callback) {
            collection.insert (doc, { w:1 }, callback);
        }, function (err) {
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

                if (sample.length != target.length)
                    return callback (new Error ('mingydb produced incorrect number of records'));

                for (var i=0,j=sample.length; i<j; i++) {
                    var candidate = sample[i];
                    delete candidate._id;
                    var found = false;
                    for (var k in target)
                        if (matchLeaves (candidate, target[k], true)) {
                            found = true;
                            break;
                        }
                    if (!found) {
                        console.log (JSON.stringify (target));
                        console.log (JSON.stringify (sample));
                        return callback (new Error ('mingydb and mongodb disagreed'));
                    }
                }

                callback();
            });
        });
    });
}

describe ("Aggregation", function(){

    before (function (done) {
        mingydb.collection (
            'test-mingydb',
            'test-mingydb',
            new mingydb.Server ('127.0.0.1', 27017),
            function (err, col) {
                if (err) return done (err);
                collection = col;
                mingydb.collection (
                    'test-mingydb',
                    'test-mingydb',
                    new mingydb.Server ('127.0.0.1', 27017),
                    function (err, col) {
                        if (err) return done (err);
                        targetCollection = col;
                        done();
                    }
                );
            }
        );
    });

    describe ("#aggregate", function(){

        describe ("$match", function(){

            it ("selectively passes documents with a query", function (done) {
                testAggregation ([
                    { able:9, baker:1 },
                    { able:8, baker:2 },
                    { able:7, baker:3 },
                    { able:6, baker:4 },
                    { able:5, baker:5 },
                    { able:4, baker:6 },
                    { able:3, baker:7 },
                    { able:2, baker:8 },
                    { able:1, baker:9 }
                ], [
                    {
                        $match:     {
                            able:   { $gt:2 },
                            baker:  { $gt:2 }
                        }
                    }
                ], done);
            });

        });

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
                        'novel.field':  '$able.baker'
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
                            field:      '$able.baker'
                        }
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
                    { $skip:2 }
                ], done);
            });

        });

        describe ("$sort", function(){

            it ("sorts the input by the value of an expression", function (done) {
                testAggregation ([
                    { able:{ baker:9 }, baker:42 },
                    { able:{ baker:8 }, baker:42 },
                    { able:{ baker:7 }, baker:42, charlie:9 },
                    { able:{ baker:6 }, baker:42 },
                ], [
                    { $sort:{
                        'able.baker':   1
                    }}
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
                            { able:{ baker:7 },         foo:{ bar:9 } },
                            { able:{ baker:{ able:1 }}, foo:700 },
                            { able:{ baker:7 },         foo:{ bar:10 } },
                            { able:{ baker:7 },         foo:{ bar:9 } },
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
                                'able.baker':   1
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
                                'able.baker':   1
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

            it ("makes recursive redactions", function (done) {
                testAggregation ([
                    { able:[ 'a', 'b' ], baker:[
                        { able:[ 'a', 'b' ] },
                        { able:[ 'b', 'c' ] },
                        { able:[ 'c', 'd' ] },
                        { able:[ 'd', 'b' ] }
                    ] },
                    { able:[ 'b', 'c' ], baker:[
                        { able:[ 'a', 'b' ] },
                        { able:[ 'b', 'c' ] },
                        { able:[ 'c', 'd' ] },
                        { able:[ 'd', 'b' ] }
                    ] },
                    { able:[ 'c', 'd' ], baker:[
                        { able:[ 'a', 'b' ] },
                        { able:[ 'b', 'c' ] },
                        { able:[ 'c', 'd' ] },
                        { able:[ 'd', 'b' ] }
                    ] },
                    { able:[ 'd', 'b' ], baker:[
                        { able:[ 'a', 'b' ] },
                        { able:[ 'b', 'c' ] },
                        { able:[ 'c', 'd' ] },
                        { able:[ 'd', 'b' ] }
                    ] },
                ], [
                    { $redact:{
                        $cond:{
                            if:         { $gt: [ { $size:{ $setIntersection:[
                                "$able",
                                [ 'b' ]
                            ] } }, 0 ] },
                            then:       '$$DESCEND',
                            'else':     '$$PRUNE'
                        }
                    } }
                ], done);
            });

        });

        describe ("$geoNear", function(){

        });

    });

});
