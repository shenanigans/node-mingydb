
/**     @module/class waxwork.Cursor
    A wrapped [cursor](mongodb.Cursor) with inline [decompression](waxwork.Compressor).
@argument/mongodb.Cursor mongoloid
    Native MongoDB cursor to wrap.

@Boolean #isClosed
*/

var async = require ('async');

function Cursor (mongoloid, compressor) {
    this.mongoloid = mongoloid;
    this.compressor = compressor;
}


/**     @member/Function #count

*/
Cursor.prototype.count = function (doSkip, callback) {
    if (!callback) {
        if (!(callback = doSkip))
            throw new Error ('callback required');
        doSkip = false;
    }

    this.mongoloid.count (doSkip, callback);
};


/**     @member/Function #nextObject

*/
Cursor.prototype.nextObject = function (callback) {
    var self = this;
    this.mongoloid.nextObject (function (err, item) {
        if (err) return callback (err);
        if (!item) return callback();
        self.compressor.decompress (item, callback);
    });
};


/**     @member/Function each

*/
Cursor.prototype.each = function (callback) {
    var self = this;
    var done = false;
    var recsIn = 0;
    var recsOut = 0;
    this.mongoloid.each (function (err, item) {
        if (err) return callback (err);
        if (!item) {
            done = true;
            if (recsIn == recsOut)
                callback();
            return;
        }

        recsIn++;
        self.compressor.decompress (item, function (err, decompressed) {
            if (err) return callback (err);
            recsOut++;
            callback (undefined, decompressed);
            if (done && recsOut == recsIn)
                process.nextTick (callback);
        });
    });
};


/**     @member/Function toArray

*/
Cursor.prototype.toArray = function (callback) {
    var self = this;
    this.mongoloid.toArray (function (err, recs) {
        if (err) return callback (err);
        if (!recs) return callback (undefined, []);
        if (!recs.length) return callback (undefined, []);

        var output = [];
        async.each (Object.keys (recs), function (recI, callback) {
            self.compressor.decompress (recs[recI], function (err, decompressedRec) {
                if (err) return callback (err);
                output[recI] = decompressedRec;
                callback();
            });
        }, function (err) {
            if (err) return callback (err);
            callback (undefined, output);
        });
    });
};


/**     @member/Function explain
    Requests a metadocument of processing info for this query. Index names containing underscores
    are assumed to be of the format generated automatically by MongoDB and an attempt will be made
    to decompress key names.
*/
Cursor.prototype.explain = function (callback) {
    var self = this;
    this.mongoloid.explain (function (err, info) {
        if (err) return callback (err);
        if (info.cursor.slice (0, 12) != 'BtreeCursor ')
            return callback (undefined, info);

        self.compressor.decompressIndexName (info.cursor.slice (12), function (err, name) {
            if (err) return callback (err);
            info.cursor = 'BtreeCursor '+name;
            callback (undefined, info);
        });
    });
};


/**     @member/Function close

*/
Cursor.prototype.close = function (callback) {
    var self = this;
    this.mongoloid.close (function (err) { callback (err, self); });
};


/**     @member/Function rewind

*/
Cursor.prototype.rewind = function(){
    this.mongoloid.rewind();
    return this;
};

module.exports = Cursor;
