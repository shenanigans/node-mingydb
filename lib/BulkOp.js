
var async = require ('async');

/**     @class mingydb.BulkOp
    @root
    A wrapped bulk operation command which executes its queued jobs in order.
@argument/mongodb.BulkOp mongoloid
*/
function BulkOp (mongoloid, compressor, callback) {
    this.compressor = compressor;
    this.mongoloid = mongoloid;
    this.callback = callback;
    this.threads = [];
}


/**     @member/Function insert

*/
BulkOp.prototype.insert = function (rec) {
    var context = new Context ('insert', rec);
    this.threads.push (context);
    return context;
};


/**     @member/Function find

*/
BulkOp.prototype.find = function (rec) {
    var context = new Context ('find', rec);
    this.threads.push (context);
    return context;
};


/**     @member/Function execute

*/
BulkOp.prototype.execute = function (callback) {
    var self = this;
    async.each (this.threads, function (thread, callback) {
        async.parallel ([
            function (callback) {
                if (thread.op == 'insert')
                    return self.compressor.compressRecord (thread.doc, function (err, compressed) {
                        if (err) return callback (err);
                        thread.doc = compressed;
                        callback();
                    });

                // find
                self.compressor.compressQuery (thread.doc, function (err, compressed) {
                    if (err) return callback (err);
                    thread.doc = compressed;
                    callback();
                });
            },
            function (callback) {
                if (!thread.option)
                    return callback();
                if (thread.job == 'replaceOne')
                    return self.compressor.compressRecord (thread.option, function (err, compressed) {
                        if (err) return callback (err);
                        thread.option = compressed;
                        callback();
                    });

                if (thread.job == 'remove' || thread.job == 'removeOne')
                    return self.compressor.compressQuery (thread.options, function (err, compressed) {
                        if (err) return callback (err);
                        thread.options = compressed;
                        callback();
                    });

                self.compressor.compressUpdate (thread.option, function (err, compressed) {
                    if (err) return callback (err);
                    thread.option = compressed;
                    callback();
                });
            }
        ], callback);
    }, function (err) {
        if (err) return callback (err);

        for (var i=0,j=self.threads.length; i<j; i++) {
            var thread = self.threads[i];
            var mongoloidThread = self.mongoloid[thread.op] (thread.doc);
            if (thread.doUpsert)
                mongoloidThread = mongoloidThread.upsert();
            if (thread.job)
                if (thread.option)
                    mongoloidThread[thread.job] (thread.option);
                else
                    mongoloidThread[thread.job]();
        }

        self.mongoloid.execute (function (err, results) {
            if (self.callback)
                self.callback (err, results);
            callback (err, results);
        });
    });
};


/**     @class Context

*/
function Context (op, doc) {
    this.op = op;
    this.doc = doc;
}


/**     @member/Function Context#remove
    Remove all selected documents from the database.
*/
Context.prototype.remove = function(){
    this.job = 'remove';
};


/**     @member/Function Context#removeOne
    Remove the first document from the database.
*/
Context.prototype.removeOne = function(){
    this.job = 'removeOne';
};


/**     @member/Function Context#replaceOne
    Replaces the first document with another document.
*/
Context.prototype.replaceOne = function (option) {
    this.job = 'replaceOne';
    this.option = option;
};


/**     @member/Function Context#update
    Updates every matching document.
*/
Context.prototype.update = function (option) {
    this.job = 'update';
    this.option = option;
};


/**     @member/Function Context#updateOne
    Updates the first matching document.
*/
Context.prototype.updateOne = function (option) {
    this.job = 'updateOne';
    this.option = option;
};


/**     @member/Function Context#upsert
    Sets the `upsert` option to `true`.
*/
Context.prototype.upsert = function(){
    this.doUpsert = true;
    return this;
};


module.exports = BulkOp;
