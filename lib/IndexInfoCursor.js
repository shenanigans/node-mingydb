
/**     @class waxwork.IndexInfoCursor
    @root
    A wrapped [IndexInfoCursor](mongodb.IndexInfoCursor) with inline [decompression](waxwork.Compressor).
@argument/mongodb.IndexInfoCursor mongoloid
    Native MongoDB IndexInfoCursor to wrap.

@Boolean #isClosed
*/

var async = require ('async');

function IndexInfoCursor (mongoloid, compressor) {
    this.mongoloid = mongoloid;
    this.compressor = compressor;
}


/**     @member/Function #count

*/
IndexInfoCursor.prototype.count = function (doSkip, callback) {
    if (!callback) {
        if (!(callback = doSkip))
            throw new Error ('callback required');
        doSkip = false;
    }

    this.mongoloid.count (doSkip, callback);
};


/**     @member/Function close

*/
IndexInfoCursor.prototype.close = function (callback) {
    var self = this;
    this.mongoloid.close (function (err) { callback (err, self); });
};


/**     @member/Function rewind

*/
IndexInfoCursor.prototype.rewind = function(){
    this.mongoloid.rewind();
    return this;
};


/**     @member/Function #nextObject

*/
IndexInfoCursor.prototype.nextObject = function (callback) {
    var self = this;
    this.mongoloid.nextObject (function (err, spec) {
        if (err) return callback (err);
        if (!spec) return callback();

        var paths = Object.keys (spec.key);
        var pathsIS = Object.keys (paths);
        var newPaths = [];
        async.each (pathsIS, function (pathsI, callback) {
            var path = paths[pathsI];

            if (path == '_id') {
                newPaths[pathsI] = '_id';
                return callback();
            }

            self.compressor.decompressPath (path, function (err, decompressedPath) {
                if (err) return callback (err);
                newPaths[pathsI] = decompressedPath;
                callback();
            });
        }, function (err) {
            if (err) return callback (err);

            // establish what the calculated name for comparison
            var shortPathFrags = [];
            for (var key in spec.key) {
                shortPathFrags.push (key);
                shortPathFrags.push (spec.key[key]);
            }
            var calculatedName = shortPathFrags.join ('_');

            // overwrite the spec .key field
            var newKeySpec = {};
            for (var i in newPaths)
                newKeySpec[newPaths[i]] = spec.key[paths[i]];
            spec.key = newKeySpec;

            // check the calculated name against the index name and replace if necessary
            if (calculatedName == spec.name) {
                var newNameFrags = [];
                for (var i in newPaths) {
                    var newPath = newPaths[i];
                    newNameFrags.push (newPath);
                    newNameFrags.push (spec.key[newPath]);
                }
                spec.name = newNameFrags.join ('_');
            }

            callback (undefined, spec);
        });
    });
};


/**     @member/Function each

*/
IndexInfoCursor.prototype.each = function (callback) {
    var self = this;
    var done = false;
    var defsIn  = 0;
    var defsOut = 0;
    this.mongoloid.each (function (err, spec) {
        if (err) return callback (err);
        if (!spec) {
            done = true;
            if (defsIn == defsOut)
                callback();
            return;
        }

        defsIn++;
        var paths = Object.keys (spec.key);
        var pathsIS = Object.keys (paths);
        var newPaths = [];
        async.each (pathsIS, function (pathsI, callback) {
            var path = paths[pathsI];

            if (path == '_id') {
                newPaths[pathsI] = '_id';
                return callback();
            }

            self.compressor.decompressPath (path, function (err, decompressedPath) {
                if (err) return callback (err);
                newPaths[pathsI] = decompressedPath;
                callback();
            });
        }, function (err) {
            if (err) return callback (err);

            // establish what the calculated name for comparison
            var shortPathFrags = [];
            for (var key in spec.key) {
                shortPathFrags.push (key);
                shortPathFrags.push (spec.key[key]);
            }
            var calculatedName = shortPathFrags.join ('_');

            // overwrite the spec .key field
            var newKeySpec = {};
            for (var i in newPaths)
                newKeySpec[newPaths[i]] = spec.key[paths[i]];
            spec.key = newKeySpec;

            // check the calculated name against the index name and replace if necessary
            if (calculatedName == spec.name) {
                var newNameFrags = [];
                for (var i in newPaths) {
                    var newPath = newPaths[i];
                    newNameFrags.push (newPath);
                    newNameFrags.push (spec.key[newPath]);
                }
                spec.name = newNameFrags.join ('_');
            }

            defsOut++;
            callback (undefined, spec);
            if (done && defsOut == defsIn)
                process.nextTick (callback);
        });
    });
};


/**     @member/Function toArray

*/
IndexInfoCursor.prototype.toArray = function (callback) {
    var self = this;
    this.mongoloid.toArray (function (err, recs) {
        if (err) return callback (err);
        if (!recs) return callback (undefined, []);
        if (!recs.length) return callback (undefined, []);

        async.each (recs, function (spec, callback) {
            var paths = Object.keys (spec.key);
            var pathsIS = Object.keys (paths);
            var newPaths = [];
            async.each (pathsIS, function (pathsI, callback) {
                var path = paths[pathsI];

                if (path == '_id') {
                    newPaths[pathsI] = '_id';
                    return callback();
                }

                self.compressor.decompressPath (path, function (err, decompressedPath) {
                    if (err) return callback (err);
                    newPaths[pathsI] = decompressedPath;
                    callback();
                });
            }, function (err) {
                if (err) return callback (err);

                // establish what the calculated name for comparison
                var shortPathFrags = [];
                for (var key in spec.key) {
                    shortPathFrags.push (key);
                    shortPathFrags.push (spec.key[key]);
                }
                var calculatedName = shortPathFrags.join ('_');

                // overwrite the spec .key field
                var newKeySpec = {};
                for (var i in newPaths)
                    newKeySpec[newPaths[i]] = spec.key[paths[i]];
                spec.key = newKeySpec;

                // check the calculated name against the index name and replace if necessary
                if (calculatedName == spec.name) {
                    var newNameFrags = [];
                    for (var i in newPaths) {
                        var newPath = newPaths[i];
                        newNameFrags.push (newPath);
                        newNameFrags.push (spec.key[newPath]);
                    }
                    spec.name = newNameFrags.join ('_');
                }

                callback();
            });
        }, function (err) {
            if (err) return callback (err);
            callback (undefined, recs);
        });
    });
};

module.exports = IndexInfoCursor;
