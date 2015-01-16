
/**     @module/class mingydb.Collection
    @super mongodb.Collection
    A wrapped MongoDB collection with record compression inlined. This is the preferred method of
    using the [this.compressor](waxwork.this.compressor). You may upgrade a native [collection]
    (mongodb.collection) to a [Collection](.Collection) at any time.
@argument/String collectionName
@argument/mongodb.Collection collection
@argument/mingydb.Compressor compressor
*/
var async                   = require ('async');
var fauxmongo               = require ('fauxmongo');
var Cursor                  = require ('./Cursor');
var IndexInfoCursor         = require ('./IndexInfoCursor');
var OrderedBulkOp           = require ('./OrderedBulkOp');
var parallelCollectionScan  = require ('./ParallelCollectionScan');

function Collection (collectionName, collection, compressor) {
    this.name = collectionName;
    this.collection = collection;
    this.compressor = compressor;
}


/**     @member/Function insert
    Compress and insert a record into the database. Unlike native insertion, the callback is not
    optional. This is because the record [compression](waxwork.this.compressor) stage may generate
    an Error.
@argument/Object record
@argument/Object options
    optional
@callback
    @argument/Error|undefined err
*/
Collection.prototype.insert = function (recs, options, callback) {
    if (typeof options == 'function') {
        callback = options;
        options = {};
    }
    if (!(recs instanceof Array))
        recs = [ recs ];

    var self = this;
    async.each (recs, function (rec, callback) {
        self.compressor.compressRecord (rec, function (err, compressed) {
            if (err) return callback (err);
            self.collection.insert (compressed, options, callback);
        });
    }, callback);
};


/**     @member/Function find
    Compress a query and pass it to the database, producing a [cursor](mingydb.Cursor).
@argument/Object query
@argument/Object fields
    @optional
    Projection specification document. Key paths will be compressed before query submission.
@argument/Object options
    @optional
    Pass native options to MongoDB. The `sort` and `hint` options will be compressed.
@callback
    @argument/Error|undefined err
    @argument/mingydb.Cursor|undefined cursor
*/
Collection.prototype.find = function (query, fields, options, callback) {
    if (arguments.length == 1) {
        callback = query;
        query = {};
        fields = undefined;
        options = {};
    } else if (arguments.length == 2) {
        callback = fields;
        options = {};
        fields = undefined;
    } else if (arguments.length == 3) {
        callback = options;
        options = {};
    }

    if (!callback)
        throw new Error ('callback required');

    var self = this;
    var outgoingQuery;
    var jobs = [ function (callback) {
        self.compressor.compressQuery (query, function (err, compressedQuery) {
            outgoingQuery = compressedQuery;
            callback();
        });
    } ];

    if (fields || options.fields)
        jobs.push (function (callback) {
            self.compressor.compressProjection (fields || options.fields, function (err, cProj) {
                if (err) return callback (err);
                options.fields = cProj;
                callback();
            });
        });

    if (options.sort)
        jobs.push (function (callback) {
            self.compressor.compressSort (
                options.sort,
                function (err, compressedSort) {
                    if (err) return callback (err);
                    options.sort = compressedSort;
                    callback();
                }
            );
        });

    if (options.hint)
        jobs.push (function (callback) {
            self.compressor.compressIndexSpec (
                options.hint,
                function (err, compressedHint) {
                    if (err) return callback (err);
                    options.hint = compressedHint;
                    callback();
                }
            );
        });

    async.parallel (jobs, function (err) {
        if (err) return callback (err);
        self.collection.find (
            outgoingQuery,
            fields,
            options,
            function (err, cursor) {
                if (err) return callback (err);
                callback (undefined, new Cursor (cursor, self.compressor));
            }
        );
    });
};


/**     @member/Function Collection#findAll
    A convenience method to call [find](.Collection#find) and retrieve all records as an Array.
    Optionally project or pass a wide variety of [options](.Collection.QueryOptions).

    **DANGER** - like toArray, you can run the process out of memory if your query selects tons of
    documents. It is *highly* recommended that you use use the `limit` option to control the return
    batch.
@argument/Object query
@argument/Object fields
    @optional
@argument/.Collection.QueryOptions options
    @optional
    @
@callback
    @argument/Error|undefined err
    @argument/Array records
*/
Collection.prototype.findAll = function (query, fields, options, callback) {
    if (arguments.length == 2) {
        callback = fields;
        options = {};
        fields = undefined;
    } else if (arguments.length == 3) {
        callback = options;
        options = fields;
        fields = undefined;
    }

    if (!callback)
        throw new Error ('callback required');

    var self = this;
    var outgoingQuery;
    var jobs = [ function (callback) {
        self.compressor.compressQuery (query, function (err, compressedQuery) {
            outgoingQuery = compressedQuery;
            callback (err);
        });
    } ];

    if (fields || options.fields)
        jobs.push (function (callback) {
            self.compressor.compressProjection (fields || options.fields, function (err, cProj) {
                if (err) return callback (err);
                options.fields = cProj;
                callback();
            });
        });

    if (options.sort)
        jobs.push (function (callback) {
            self.compressor.compressSort (
                options.sort,
                function (err, compressedSort) {
                    if (err) return callback (err);
                    options.sort = compressedSort;
                    callback();
                }
            );
        });

    if (options.hint)
        jobs.push (function (callback) {
            self.compressor.compressIndexSpec (
                options.hint,
                function (err, compressedHint) {
                    if (err) return callback (err);
                    options.hint = compressedHint;
                    callback();
                }
            );
        });

    async.parallel (jobs, function (err) {
        if (err) return callback (err);
        self.collection.find (
            outgoingQuery,
            options,
            function (err, cursor) {
                if (err) return callback (err);
                cursor.toArray (function (err, records) {
                    if (err) return callback (err);
                    if (!records.length) return callback (records);
                    if (options && options.raw)
                        return callback (undefined, records);

                    var output = [];
                    async.each (Object.keys (records), function (recI, callback) {
                        self.compressor.decompress (
                            records[recI],
                            function (err, decompressedRec) {
                                if (err) return callback (err);
                                output[recI] = decompressedRec;
                                callback();
                            }
                        );
                    }, function (err) {
                        if (err) return callback (err);
                        callback (undefined, output);
                    });
                });
            }
        );
    });
};


/**     @member/Function Collection#findOne
    Compress a query document and query the database for zero or one matching records. Optionally
    project the result record.

    Note that contrary to the native driver documentation, in `mingydb` the second argument is
    always assumed to be a projection, not an options object.
@argument/Object query
@argument/Object fields
    @optional
@argument/Object options
    @optional
@argument/Object skip
    @optional
@argument/Object limit
    @optional
@argument/Object timeout
    @optional
@callback
    @argument/Error|undefined err
    @argument/Object|undefined rec
*/
Collection.prototype.findOne = function (query, fields, skip, limit, timeout, options, callback) {
    // sort out the arguments
    if (arguments.length == 2) {
        callback = fields;
        options = {};
        fields = skip = limit = timeout = undefined;
    } else if (arguments.length == 3) {
        callback = skip;
        options = {};
        skip = limit = timeout = undefined;
    } else if (arguments.length == 4) {
        callback = limit;
        options = skip;
        limit = timeout = undefined;
    } else if (arguments.length == 5) {
        callback = timeoout;
        options = limit;
        timeout = undefined;
    } else if (arguments.length == 6) {
        callback = options;
        options = timeout;
    }

    if (!callback)
        throw new Error ('callback function required');

    var self = this;
    var outgoingQuery;
    this.compressor.compressQuery (query, function (err, compressedQuery) {
        if (err) return callback (err);

        var jobs = [];

        if (fields)
            jobs.push (function (callback) {
                self.compressor.compressQuery (
                    fields,
                    function (err, compressedFields) {
                        if (err) return callback (err);
                        fields = compressedFields;
                        callback();
                    }
                );
            });

        if (options) {
            if (options.sort)
                jobs.push (function (callback) {
                    self.compressor.compressSort (
                        options.sort,
                        function (err, compressedSort) {
                            if (err) return callback (err);
                            options.sort = compressedSort;
                            callback();
                        }
                    );
                });

        }

        function finalCall (err, rec) {
            if (err) return callback (err);
            if (!rec) return callback();
            if (options && options.raw)
                return callback (undefined, rec);
            self.compressor.decompress (rec, callback);
        }

        if (!jobs.length)
            return self.collection.findOne (compressedQuery, fields, options, finalCall);

        async.parallel (jobs, function (err) {
            if (err) return callback (err);
            self.collection.findOne (compressedQuery, fields, options, finalCall);
        });
    });
};


/**     @member/Function update

*/
Collection.prototype.update = function (query, update, options, callback) {
    if (typeof options == 'function') {
        callback = options;
        options = { w:callback ? 1 : 0 };
    }
    if (!Object.hasOwnProperty.call (options, 'w'))
        options.w = callback ? 1 : 0;

    var self = this;
    async.parallel ([
        function (callback) {
            self.compressor.compressQuery (query, function (err, compressedQuery) {
                query = compressedQuery;
                callback (err);
            });
        },
        function (callback) {
            self.compressor.compressUpdate (update, function (err, compressedUpdate) {
                update = compressedUpdate;
                callback (err);
            });
        }
    ], function (err) {
        if (err) return callback (err);
        self.collection.update (query, update, options, callback);
    });
};


/**     @member/Function Collection#findAndModify

*/
Collection.prototype.findAndModify = function (query, sort, update, options, callback) {
    if (arguments.length == 3) {
        callback = update;
        update = sort;
        options = {};
        sort = undefined;
    } else if (arguments.length == 4) {
        callback = options;
        options = {};
    }

    if (!callback)
        throw new Error ('callback function required');

    var self = this;
    var outgoingQuery, outgoingUpdate;
    var jobs = [
        function (callback) {
            self.compressor.compressQuery (query, function (err, compressedQuery) {
                outgoingQuery = compressedQuery;
                callback (err);
            });
        },
        function (callback) {
            self.compressor.compressUpdate (update, function (err, compressedUpdate) {
                outgoingUpdate = compressedUpdate;
                callback (err);
            });
        }
    ];

    if (sort || options.sort)
        jobs.push (function (callback) {
            self.compressor.compressSort (
                sort || options.sort,
                function (err, compressedSort) {
                    if (err) return callback (err);
                    sort = compressedSort;
                    callback();
                }
            );
        });

    if (options.fields)
        jobs.push (function (callback) {
            self.compressor.compressProjection (
                options.fields,
                function (err, compressedFields) {
                    if (err) return callback (err);
                    options.fields = compressedFields;
                    callback();
                }
            );
        });

    async.parallel (jobs, function (err) {
        if (err) return callback (err);
        self.collection.findAndModify (
            outgoingQuery,
            sort,
            outgoingUpdate,
            options,
            function (err, rec) {
                if (err) return callback (err);
                if (!rec) return callback();
                if (options.raw)
                    return callback (undefined, rec);
                self.compressor.decompress (rec, callback);
            }
        );
    });
};


/**     @member/Function Collection#remove

*/
Collection.prototype.remove = function (query, options, callback) {
    if (arguments.length == 1) {
        callback = query;
        query = options = undefined;
    } else if (arguments.length == 2) {
        callback = options;
        options = {};
    }

    var self = this;
    this.compressor.compressQuery (query, function (err, compressed) {
        if (err) {
            if (callback) callback (err);
            return;
        }
        if (!callback) {
            self.collection.remove (compressed, options);
            return;
        }
        if (!options.w) options.w = 1;
        self.collection.remove (compressed, options, callback);
    });
};


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
function deepCompare (able, baker) {
    if (able === baker) return true;
    var type = getTypeStr (able);
    if (type != getTypeStr (baker)) return false;
    if (type == 'object' || type == 'array') {
        if (type == 'object') {
            if (Object.keys (able).length != Object.keys (baker).length) return false;
        } else if (able.length != baker.length) return false;
        for (var key in able)
            if (!deepCompare (able[key], baker[key])) return false;
        return true;
    }
    return able == baker;
}
/**     @member/Function Collection#distinct

*/
Collection.prototype.distinct = function (key, query, options, callback) {
    if (arguments.length == 2) {
        callback = query;
        query = options = undefined;
    } else if (arguments.length == 3) {
        callback = options;
        options = undefined;
    }

    var docQuery;
    if (query) {
        var keys = Object.keys (query);
        if (keys[0] && keys[0][0] == '$') {
            docQuery = {};
            docQuery[key] = query;
        } else
            docQuery = query;
    } else {
        docQuery = {};
        docQuery[key] = { $exists:true };
    }
    var docFields = { _id:false };
    docFields[key] = true;

    var self = this;
    var compressedQuery, compressedPath;
    var jobs = [ function (callback) {
        self.compressor.compressPath (key, function (err, compressed) {
            if (err) return callback (err);
            compressedPath = compressed;
            callback();
        });
    } ];

    if (query) jobs.push (function (callback) {
        self.compressor.compressQuery (query, function (err, compressed) {
            if (err) return callback (err);
            compressedQuery = compressed;
            callback();
        });
    });

    async.parallel (jobs, function (err) {
        if (err) return callback (err);

        function distinctReactor (err, vals) {
            if (err) return callback (err);
            if (!vals) return callback (undefined, []);

            var output = [];
            async.each (vals, function (val, callback) {
                var type = getTypeStr (val)
                if (type != 'object' && type != 'array') {
                    output.push (val);
                    return callback();
                }

                self.compressor.decompress (key, val, function (err, decompressedVal) {
                    if (err) return callback (err);
                    output.push (decompressedVal);
                    callback();
                });
            }, function (err) {
                callback (err, err ? undefined : output);
            });
        }

        if (query)
            self.collection.distinct (compressedPath, compressedQuery, distinctReactor);
        else
            self.collection.distinct (compressedPath, distinctReactor);
    });
};


/**     @member/Function Collection#count

*/
Collection.prototype.count = function (query, options, callback) {
    if (arguments.length == 1) {
        callback = query;
        query = undefined;
        options = {};
    } else if (arguments.length == 2) {
        callback = options;
        options = {};
    }

    if (!query)
        return this.collection.count (query, options, callback);

    var self = this;
    this.compressor.compressedQuery (query, function (err, compressedQuery) {
        if (err) return callback (err);
        self.collection.count (compressedQuery, options, callback);
    });
};


/**     @member/Function Collection#drop

@callback
    @argument/Error|undefined err
*/
Collection.prototype.drop = function (callback) {
    this.collection.drop (callback);
};


/**     @member/Function Collection#findAndRemove

*/
Collection.prototype.findAndRemove = function (query, sort, options, callback) {
    if (arguments.length == 2) {
        // throw new Error ('a sort specification is required');
        callback = sort;
        options = {};
        sort = undefined;
    } else if (arguments.length == 3) {
        callback = options;
        options = {};
    }

    var self = this;
    var outgoingQuery, outgoingSort;
    var jobs = [
        function (callback) {
            self.compressor.compressQuery (query, function (err, compressedQuery) {
                outgoingQuery = compressedQuery;
                callback (err);
            });
        }
    ];
    if (sort) jobs.push (function (callback) {
        self.compressor.compressSort (sort, function (err, compressedSort) {
            outgoingSort = compressedSort;
            callback (err);
        });
    });

    async.parallel (jobs, function (err) {
        if (err) return callback (err);
        self.collection.findAndRemove (outgoingQuery, outgoingSort, options, function (err, rec) {
            if (err || !rec)
                return callback (err);
            self.compressor.decompress (rec, callback);
        });
    });
};


/**     @member/Function Collection#ensureIndex

*/
Collection.prototype.ensureIndex = function (spec, options, callback) {
    if (arguments.length == 1) {
        callback = options = undefined;
    } else if (arguments.length == 2) {
        callback = options;
        options = undefined;
    }

    var self = this;
    this.compressor.compressIndexSpec (spec, function (err, compressedIndex) {
        if (err) return callback (err);
        self.collection.ensureIndex (compressedIndex, options, callback);
    });
};


/**     @member/Function Collection#dropIndex

*/
Collection.prototype.dropIndex = function (name, callback) {
    var self = this;
    this.compressor.compressIndexName (name, function (err, compressedName) {
        if (err) return callback (err);
        self.collection.dropIndex (compressedName, callback);
    });
};


/**     @member/Function Collection#dropAllIndexes

*/
Collection.prototype.dropAllIndexes = function (callback) {
    this.collection.dropAllIndexes (callback);
};


/**     @member/Function Collection#getIndexes

*/
Collection.prototype.listIndexes = function (options) {
    return new IndexInfoCursor (this.collection.listIndexes (options), this.compressor);
};


/**     @member/Function Collection#indexInformation

*/
Collection.prototype.indexInformation = function (options, callback) {
    if (arguments.length == 1) {
        callback = options;
        options = {};
    }

    var self = this;
    this.collection.indexInformation (options, function (err, info) {
        if (err) return callback (err);
        var indexNames = Object.keys (info);
        var newInfo = {};
        async.each (indexNames, function (name, callback) {
            self.compressor.decompressIndexName (name, function (err, decompressedName) {
                if (err) return callback (err);
                var specs = info[name];
                var newSpec = [];
                async.times (specs.length, function (specI, callback) {
                    var spec = specs[specI];
                    if (spec[0] == '_id') {
                        newSpec[specI] = spec;
                        return callback();
                    }
                    self.compressor.decompressPath (spec[0], function (err, decompressedPath) {
                        if (err) return callback (err);
                        newSpec[specI] = [ decompressedPath, spec[1] ];
                        callback();
                    });
                }, function (err) {
                    if (err) return callback (err);
                    newInfo[decompressedName] = newSpec;
                    callback();
                });
            });
        }, function (err) {
            callback (err, err ? undefined : newInfo);
        });
    });
};


/**     @member/Function Collection#indexExists

*/
Collection.prototype.indexExists = function (indexNames, callback) {
    if (!(indexNames instanceof Array))
        indexNames = [ indexNames ];

    var self = this;
    var compressedNames = [];
    async.each (indexNames, function (indexName, callback) {
        self.compressor.compressIndexName (indexName, function (err, indexName) {
            if (err) return callback (err);
            compressedNames.push (indexName);
            callback();
        });
    }, function (err) {
        if (err)
            return callback (err);
        self.collection.indexExists (compressedNames, callback);
    });
};

/**     @member/Function Collection#reIndex

*/
Collection.prototype.reIndex = function (callback) {
    this.collection.reIndex (callback);
};


/**     @member/Function Collection#options

*/
Collection.prototype.options = function (callback) {
    process.nextTick (function(){
        callback (new Error ('the Node.js MongoDB driver does not support #options'));
    });
};


/**     @member/Function Collection#isCapped

*/
Collection.prototype.isCapped = function (callback) {
    return this.collection.isCapped.apply (this.collection, callback);
};


/**     @member/Function Collection#geoNear

*/
Collection.prototype.geoNear = function (callback) {

};


/**     @member/Function Collection#geoHaystack

*/
Collection.prototype.geoHaystack = function (callback) {

};


/**     @member/Function Collection#aggregate

*/
Collection.prototype.aggregate = function (pipeline, options, callback) {
    if (arguments.length == 2) {
        callback = options;
        options = {};
    }

    if (!pipeline.length)
        return process.nextTick (function(){ callback (undefined, []); });

    var self = this;
    this.compressor.compressAggregationPipeline (pipeline, function (err, compressedPipeline) {
        if (err) return callback (err);

        self.collection.aggregate (compressedPipeline, function (err, result) {
            if (err) return callback (err);

            if (options.cursor)
                return callback (undefined, new Cursor (result, self.compressor));

            // decompress docs
            var output = [];
            async.each (result, function (rec, callback) {
                self.compressor.decompress (rec, function (err, decompressedRec) {
                    if (err) return callback (err);
                    output.push (decompressedRec);
                    callback();
                });
            }, function (err) {
                callback (err, err ? undefined : output);
            });
        });
    });
};


/**     @member/Function Collection#stats

*/
Collection.prototype.stats = function(){
    return this.collection.stats.apply (this.collection, arguments);
};


/**     @member/Function Collection#rename

*/
Collection.prototype.rename = function(){
    return this.collection.rename.apply (this.collection, arguments);
};


/**     @member/Function Collection#initializeOrderedBulkOp

*/
Collection.prototype.initializeOrderedBulkOp = function () {

};


/**     @member/Function Collection#parallelCollectionScan

*/
Collection.prototype.parallelCollectionScan = function () {

};


/**     @member/Function Collection#setCompressed

*/
Collection.prototype.setCompressed = function (path, callback) {
    this.compressor.setCompressed (path, callback);
};


/**     @member/Function Collection#setDecompressed

*/
Collection.prototype.setDecompressed = function (path, callback) {
    this.compressor.setDecompressed (path, callback);
};


module.exports = Collection;
