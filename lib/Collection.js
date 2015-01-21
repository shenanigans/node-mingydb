
/**     @class mingydb.Collection
    @super mongodb.Collection
    @root
    A wrapped MongoDB collection with record compression inlined.
@argument/String collectionName
@argument/mongodb.Collection collection
@argument/mingydb.Compressor compressor
*/
var async           = require ('async');
var Cursor          = require ('./Cursor');
var IndexInfoCursor = require ('./IndexInfoCursor');
var BulkOp          = require ('./BulkOp');

function Collection (collectionName, collection, compressor) {
    this.name = collectionName;
    this.collection = collection;
    this.compressor = compressor;
}


/**     @member/Function save
    Compress and save a record into the database. If the saved record has an _id field, an upsert is
    performed. Otherwise a new document is inserted.
*/
Collection.prototype.save = function (doc, options, callback) {
    if (arguments.length == 2) {
        callback = options;
        options = {};
    }

    var self = this;
    this.compressor.compressRecord (doc, function (err, compressed) {
        if (err) return callback (err);
        self.collection.save (compressed, options, callback);
    });
};


/**     @member/Function insert
    Compress and insert a record into the database. Unknown paths will be given new minification
    keys. Unlike native insertion, the callback is not optional. This is because the record
    [compression](mingydb.Compressor) stage may generate an Error.
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
@argument/Object projection
    @optional
    Projection specification document. Compressed before submission.
@argument/Object options
    @optional
    Pass native options to MongoDB. The `sort` and `hint` options will be compressed.
@callback
    @argument/Error|undefined err
    @argument/mingydb.Cursor|undefined cursor
*/
Collection.prototype.find = function(){
    var query, projection, options, callback;
    if (arguments.length == 1) {
        callback = arguments[0];;
        query = {};
        options = {};
    } else if (arguments.length == 2) {
        query = arguments[0];
        callback = arguments[1];
        options = {};
    } else if (arguments.length == 3) {
        query = arguments[0];
        projection = arguments[1];
        callback = arguments[2];
        options = {};
    } else if (arguments.length == 4) {
        query = arguments[0];
        projection = arguments[1];
        options = arguments[2];
        callback = arguments[3];
    } else if (arguments.length == 5) {
        query = arguments[0];
        projection = arguments[1];
        options = { skip:arguments[2], limit:arguments[3] };
        callback = arguments[4];
    } else {
        query = arguments[0];
        projection = arguments[1];
        skip = arguments[2];
        options = { skip:arguments[2], limit:arguments[3], timeout:arguments[4] };
        callback = arguments[5];
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

    if (projection || options.fields)
        jobs.push (function (callback) {
            self.compressor.compressProjection (projection || options.projection, function (err, cProj) {
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
            projection,
            options,
            function (err, cursor) {
                if (err) return callback (err);
                callback (undefined, new Cursor (cursor, self.compressor));
            }
        );
    });
};


/**     @member/Function findAll
    A convenience method to call [find](mingydb.Collection#find) and then [toArray]
    (mingydb.Cursor#toArray) on the resultant cursor. Optionally project or pass other `options`.

    **DANGER** - like toArray, you can run the process out of memory if your query selects tons of
    documents. It is *highly* recommended that you use use the `limit` option to control the return
    batch.
@argument/Object query
@argument/Object projection
    @optional
@argument/.Collection.QueryOptions options
    @optional
    @
@callback
    @argument/Error|undefined err
    @argument/Array records
*/
Collection.prototype.findAll = function(){
    var query, projection, options, callback;
    if (arguments.length == 1) {
        callback = arguments[0];;
        query = {};
        options = {};
    } else if (arguments.length == 2) {
        query = arguments[0];
        callback = arguments[1];
        options = {};
    } else if (arguments.length == 3) {
        query = arguments[0];
        projection = arguments[1];
        callback = arguments[2];
        options = {};
    } else if (arguments.length == 4) {
        query = arguments[0];
        projection = arguments[1];
        options = arguments[2];
        callback = arguments[3];
    } else if (arguments.length == 5) {
        query = arguments[0];
        projection = arguments[1];
        options = { skip:arguments[2], limit:arguments[3] };
        callback = arguments[4];
    } else {
        query = arguments[0];
        projection = arguments[1];
        skip = arguments[2];
        options = { skip:arguments[2], limit:arguments[3], timeout:arguments[4] };
        callback = arguments[5];
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

    if (projection || options.fields)
        jobs.push (function (callback) {
            self.compressor.compressProjection (projection || options.projection, function (err, cProj) {
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


/**     @member/Function findOne
    Compress a query document and query the database for zero or one matching records. Optionally
    project the result record.

    Note that contrary to the native driver documentation, in `mingydb` the second argument is
    always assumed to be a projection, never an options object.
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
Collection.prototype.findOne = function(){
    var query, projection, options, callback;
    if (arguments.length == 1) {
        callback = arguments[0];;
        query = {};
        options = {};
    } else if (arguments.length == 2) {
        query = arguments[0];
        callback = arguments[1];
        options = {};
    } else if (arguments.length == 3) {
        query = arguments[0];
        projection = arguments[1];
        callback = arguments[2];
        options = {};
    } else if (arguments.length == 4) {
        query = arguments[0];
        projection = arguments[1];
        options = arguments[2];
        callback = arguments[3];
    } else if (arguments.length == 5) {
        query = arguments[0];
        projection = arguments[1];
        options = { skip:arguments[2], limit:arguments[3] };
        callback = arguments[4];
    } else {
        query = arguments[0];
        projection = arguments[1];
        skip = arguments[2];
        options = { skip:arguments[2], limit:arguments[3], timeout:arguments[4] };
        callback = arguments[5];
    }

    if (!callback)
        throw new Error ('callback function required');

    var self = this;
    var outgoingQuery;
    this.compressor.compressQuery (query, function (err, compressedQuery) {
        if (err) return callback (err);

        var jobs = [];

        if (projection)
            jobs.push (function (callback) {
                self.compressor.compressQuery (
                    projection,
                    function (err, compressedProjection) {
                        if (err) return callback (err);
                        projection = compressedProjection;
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
            return self.collection.findOne (compressedQuery, projection, options, finalCall);

        async.parallel (jobs, function (err) {
            if (err) return callback (err);
            self.collection.findOne (compressedQuery, projection, options, finalCall);
        });
    });
};


/**     @member/Function update
    Compress and submit an update to the database.
@argument/Object query
@argument/Object update
@argument/JSON options
    @optional
@callback
    @argument/Error|undefined err
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


/**     @member/Function findAndModify
    Compress and submit an update to the database and retrieve the affected record atomically.
@argument/Object query
@argument/Object sort
@argument/Object update
@argument/JSON options
    @optional
@callback
    @argument/Error|undefined err
    @argument/Object|undefined rec
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


/**     @member/Function remove
    Compress a query, submit it to the database and delete all matching records.
@argument/Object query
@argument/JSON options
    @optional
@callback
    @argument/Error|undefined err
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
/**     @member/Function distinct
    Scan a collection or query selection, collecting all unique values and/or subdocuments.
@argument/String key
    The path to scan.
@argument/Object query
    @optional
    Selects a subset of documents to scan for unique values.
@argument/JSON options
    @optional
@callback
    @argument/Error|undefined err
    @argument/Array values
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


/**     @member/Function count
    Compress a query, select and count matching records.
@argument/Object query
@argument/JSON options
@callback
    @argument/Error|undefined err
    @argument/Number count
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
    Drop the entire collection.
@callback
    @argument/Error|undefined err
*/
Collection.prototype.drop = function (callback) {
    this.collection.drop (callback);
};


/**     @member/Function findAndRemove
    Compress a query, select one document, remove it from the database and retrieve its contents.
@argument/Object query
@argument/Object sort
@argument/JSON options
    @optional
@callback
    @argument/Error|undefined err
    @argument/Object|undefined rec
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


/**     @member/Function ensureIndex
    Compress an index specification and ensure that it exists on the database.
@argument/Object spec
@argument/JSON options
    @optional
@callback
    @argument/Error|undefined err
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


/**     @member/Function dropIndex
    Remove an index from the database by name.
@argument/String name
@callback
    @argument/Error|undefined err
*/
Collection.prototype.dropIndex = function (name, callback) {
    var self = this;
    this.compressor.compressIndexName (name, function (err, compressedName) {
        if (err) return callback (err);
        self.collection.dropIndex (compressedName, callback);
    });
};


/**     @member/Function dropAllIndexes
    Remove all indexes from the database.
@callback
    @argument/Error|undefined err
*/
Collection.prototype.dropAllIndexes = function (callback) {
    this.collection.dropAllIndexes (callback);
};


/**     @member/Function listIndexes
    Creates a cursor for exploring index information on this collection.
@argument/JSON options
@returns/mingydb.IndexInfoCursor
*/
Collection.prototype.listIndexes = function (options) {
    return new IndexInfoCursor (this.collection.listIndexes (options), this.compressor);
};


/**     @member/Function indexInformation
    Acquire a document describing all indexes on this collection.
@argument/JSON options
    @optional
@callback
    @argument/Error|undefined err
    @argument/Object info
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


/**     @member/Function indexExists
    Determine whether one or more indexes exist on this collection.
@argument/String|Array[String] indexNames
@callback
    @argument/Error|undefined err
    @argument/Boolean exists
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

/**     @member/Function reIndex
    Rebuild all indexes on this collection.
@callback
    @argument/Error|undefined err
*/
Collection.prototype.reIndex = function (callback) {
    this.collection.reIndex (callback);
};


/**     @member/Function options

*/
Collection.prototype.options = function(){
    return this.collection.options.apply (this.collection, arguments);
};


/**     @member/Function isCapped
    Determine whether this collection is capped.
@callback
    @argument/Error|undefined err
    @argument/Boolean capped
*/
Collection.prototype.isCapped = function (callback) {
    return this.collection.isCapped.apply (this.collection, callback);
};


/**     @member/Function geoNear
    Search for documents located near a legacy coordinate pair or GeoJSON location document.
    Requires that this collection have exactly one geospatial index of either `2d` or `2dsphere`
    type.
@argument/Object geoJSON
    @optional
@argument/Number x
    @optional
@argument/Number y
    @optional
@argument/Object options
    @optional
@callback
    @argument/Error|undefined err
    @argument/Array[Object] documents
*/
Collection.prototype.geoNear = function (x, y, options, callback) {
    var geoPoint;
    if (arguments.length == 2) {
        // geoJSON
        geoPoint = x;
        callback = y;
        x = y = undefined;
        options = {};
    } else if (arguments.length == 3) {
        if (typeof x == 'object') {
            // geoJSON
            geoPoint = x;
            callback = options || {};
            options = y;
            x = y = undefined;
        } else {
            callback = options;
            options = {};
        }
    }

    var self = this;
    function finalCall (err, results) {
        if (err) return callback (err);
        results = results instanceof Array ? results : results.results;

        async.times (results.length, function (recI, callback) {
            var rec = results[recI];
            self.compressor.decompress (rec.obj, function (err, decompressed) {
                if (err) return callback (err);
                rec.obj = decompressed;
                callback();
            });
        }, function (err) {
            callback (err, err ? undefined : results);
        });
    }

    if (options.query)
        return this.compressor.compressQuery (options.query, function (err, compressedQuery) {
            if (err) return callback (err);
            if (!geoPoint) {
                // legacy point
                // shallow clone the options, add the query and go
                var newOptions = {};
                for (var key in options)
                    newOptions[key] = options[key];
                newOptions.query = compressedQuery;
                return self.collection.geoNear (x, y, newOptions, finalCall);
            }

            // geoJSON query
            // manual command
            var command = {
                geoNear:    self.collection.collectionName,
                near:       geoPoint
            };
            for (var key in options)
                command[key] = options[key];
            command.query = compressedQuery;
            self.collection.db.command (command, finalCall);
        });

    if (!geoPoint)
        return this.collection.geoNear (x, y, options, finalCall);

    // geoJSON query
    // manual command
    var command = {
        geoNear:    this.collection.collectionName,
        near:       geoPoint
    };
    for (var key in options)
        command[key] = options[key];
    this.collection.db.command (command, finalCall);
};


/**     @member/Function geoHaystackSearch
    Search for documents located near a legacy coordinate pair using a Haystack index. Requires that
    this collection have exactly one geospatial index of `geoHaystack` type.
@argument/Number x
@argument/Number y
@argument/Object options
    @optional
@callback
    @argument/Error|undefined err
    @argument/Array[Object] documents
*/
Collection.prototype.geoHaystackSearch = function (x, y, options, callback) {
    if (arguments.length == 3) {
        callback = options;
        options = {};
    }

    var self = this;


    if (!options.search)
        return process.nextTick (function(){
            callback (new Error ('the `search` option is required'));
        });

    this.compressor.compressQuery (options.search, function (err, compressedQuery) {
        if (err) return callback (err);

        var newOptions = {};
        for (var key in options)
            newOptions[key] = options[key];
        newOptions.search = compressedQuery;
        self.collection.geoHaystackSearch (x, y, newOptions, function (err, results) {
            if (err) return callback (err);

            var output = [];
            async.times (results.results.length, function (recI, callback) {
                self.compressor.decompress (results.results[recI], function (err, decompressed) {
                    if (err) return callback (err);
                    output[recI] = decompressed;
                    callback();
                });
            }, function (err) {
                callback (err, err ? undefined : output);
            });
        });
    });
};


/**     @member/Function aggregate
    Compress and submit an aggregation pipeline to the database. Decompress and retrieve the
    results.
@argument/Array[Object] pipeline
@argument/Object options
    @optional
@callback
    @argument/Error|undefined err
    @argument/Array[Object] documents
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


/**     @member/Function stats
    Obtain database stats information.
@argument/JSON options
@callback
    @argument/Error|undefined err
    @argument/Object stats
*/
Collection.prototype.stats = function(){
    return this.collection.stats.apply (this.collection, arguments);
};


/**     @member/Function rename
    Rename this collection.
@argument/String name
@argument/JSON options
    @optional
@callback
    @argument/Error|undefined err
*/
Collection.prototype.rename = function (name, options, callback) {
    if (arguments.length == 2) {
        callback = options;
        options = {};
    }

    var self = this;
    this.collection.rename (name, options, function (err) {
        if (err) return callback (err);
        var compressor = new Compressor (self.collection.db.databaseName, name);
        compressor.ready (function (err) {
            if (err) return callback (err);
            self.compressor = compressor;
            callback();
        });
    });
};


/**     @member/Function initializeOrderedBulkOp
    Initialize a new ordered bulk operation. Both documented callbacks in the driver are supported
    so you may pass one both at instantiation and execution. They will both execute.
@argument/Object options
    @optional
@callback
    @optional
    @argument/Error|undefined err
    @argument/Object results
        The native driver may not fully support the results driver, presenting `0` and `[]` for all
        properties in some environments.
*/
Collection.prototype.initializeOrderedBulkOp = function (options, callback) {
    var mongoloid;
    if (arguments.length == 0)
        mongoloid = this.collection.initializeOrderedBulkOp();
    else {
        if (arguments.length == 1 && typeof options == 'function') {
            callback = options;
            options = {};
        }
        mongoloid = this.collection.initializeOrderedBulkOp (options);
    }
    return new BulkOp (mongoloid, this.compressor, callback);
};


/**     @member/Function initializeUnorderedBulkOp
    Initialize a new unordered bulk operation. Both documented callbacks in the driver are supported
    so you may pass one both at instantiation and execution. They will both execute.
@argument/Object options
    @optional
@callback
    @optional
    @argument/Error|undefined err
    @argument/Object results
        The native driver may not fully support the results driver, presenting `0` and `[]` for all
        properties in some environments.
*/
Collection.prototype.initializeUnorderedBulkOp = function (options, callback) {
    var mongoloid;
    if (arguments.length == 0)
        mongoloid = this.collection.initializeUnorderedBulkOp();
    else {
        if (arguments.length == 1 && typeof options == 'function') {
            callback = options;
            options = {};
        }
        mongoloid = this.collection.initializeUnorderedBulkOp (options);
    }
    return new BulkOp (mongoloid, this.compressor, callback);
};


/**     @member/Function parallelCollectionScan

*/
Collection.prototype.parallelCollectionScan = function () {

};


/**     @member/Function setCompressed

*/
Collection.prototype.setCompressed = function (path, callback) {
    this.compressor.setCompressed (path, callback);
};


/**     @member/Function setUncompressed

*/
Collection.prototype.setUncompressed = function (path, callback) {
    this.compressor.setUncompressed (path, callback);
};


module.exports = Collection;
