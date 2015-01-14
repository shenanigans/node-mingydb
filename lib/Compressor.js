
/**     @module waxwork.Compressor

*/

var async               = require ('async');
var cachew              = require ('cachew');
var getTypeStr          = require ('likeness').getTypeStr;
var uid                 = require ('infosex').uid;
var Database            = require ('./Database');
var CompressionError    = require ('./CompressionError');

/**     @local/Object self.documentClasses
    Document class records are always cached permenantly.
*/
var documentClasses = {};

var minsCollection;

var MINSET =
    'abcdefghijklmnopqrstuvwxyz'
  + 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
  + '0123456789 '
  + '@!"#%&()[]{}*+?\\'
  + "',-_/:;<=>^`|~"
  ;
var MINSET_LEN = MINSET.length;

/**     @local/Function getMin
    @development
    @private
    Get a minification key for a number.
*/
var ID_SKIP_NUM = 36273;
function getMin (n) {
    if (n >= ID_SKIP_NUM) n++; // skip '_id'
    var out = '';
    do {
        n--;
        var digit = n % MINSET_LEN;
        out += MINSET[digit];
        n -= digit;
        n = Math.floor (n / MINSET_LEN);
    } while (n);
    return out;
}


/**     @class Configuration
@Number #cacheMinifications
    How many key minifcations should be cached locally?
@Number #mincacheDuration
    Expire path minifications (in milliseconds). Keys do not expire by default.
@Number #collisionPoll
    How many milliseconds to wait between polling queries while waiting for another server to finish
    establishing a new key minification already in progress.
@Number #collisionTimeout
    The maximum time, in milliseconds, beyond which it is assumed that the server attempting to
    establish the key has failed. This Compressor will then begin its own attempt to establish the
    key.
*/
var DefaultConfig = {
    cacheMinifications:     100000,
    mincacheDuration:       0,
    collisionPoll:          10,
    collisionTimeout:       1000 * 2, // 2 seconds
};

function Compressor (dbname, documentClass, config) {
    this.dbname = dbname;
    this.documentClass = documentClass;
    this.config = {};
    if (key)
        for (var key in DefaultConfig)
            this.config[key] = config[key] || DefaultConfig[key];
    else
        this.config = DefaultConfig;

    this.minificationCache = new cachew.DocumentChainCache (
        [ 'L', 'S' ],
        this.config.cacheMinifications,
        this.config.mincacheDuration,
        true
    );
}


Compressor.prototype.ready = function (callback) {
    if (this.isReady)
        return process.nextTick (callback);

    var self = this;
    Database.getRawCollection (this.dbname, 'mins', function (err, collection) {
        if (err) return process.nextTick (function(){ callback (err); });

        self.minsCollection = collection;
        async.parallel ([
            function (callback) {
                collection.ensureIndex ({ p:1, l:1 }, { unique:true, w:1 }, callback);
            },
            function (callback) {
                collection.ensureIndex ({ p:1, s:1 }, { sparse:true, w:1 }, callback);
            },
            function (callback) {
                // find or create this document class
                var dcRec = { _id:self.documentClass, p:null, s:'', l:'' };
                self.minsCollection.update (
                    { _id:self.documentClass },
                    { $set:{ p:self.documentClass, s:'', l:'' }, $setOnInsert:{ '%':0 }},
                    { upsert:true, w:1 },
                    function (err) {
                        if (err) return callback (err);
                        self.documentClass = dcRec;
                        callback();
                    }
                );
            }
        ], function (err) {
            if (err) return callback (err);
            self.isReady = true;
            callback();
        });
    });
}


/**     @property/Function waitForShort
    @development
    @private

@argument/Object rec
@callback
    @argument/Error|undefined err
    @argument/Object|undefine rec
*/
Compressor.prototype.waitForShort = function (rec, callback) {
    var self = this;
    var now = (new Date()).getTime();
    if (!rec.c || rec.c + this.config.collisionTimeout < now)
        // colliding key attempt failed, try to overwrite it
        return self.minsCollection.findAndModify (
            { p:rec.p, l:rec.l, c:rec.c, s:null },
            { l:1 },
            { $set:{ c:now }},
            function (err, checkRec) {
                if (err) return callback (err);
                if (!checkRec)
                    return self.minsCollection.findOne ({ p:rec.p, l:rec.l }, function (err, rec) {
                        if (err) return callback (err);
                        if (!rec) return callback (new Error (
                            'object key compression record suddenly disappeared'
                        ));
                        if (rec.s) return callback (undefined, rec);
                        self.waitForShort (rec, callback);
                    });
                if (checkRec.s) return callback (undefined, checkRec);

                // now it's OUR novel key!
                self.minsCollection.findAndModify (
                    { _id:rec.p },
                    { _id:1 },
                    { $inc:{ '%':1 }},
                    { new:true },
                    function (err, rec) {
                        if (err) return callback (err);
                        if (!rec) return callback (new Error (
                            'object key compression record suddenly disappeared'
                        ));
                        var newShortKey = getMin (rec['%']);
                        self.minsCollection.update (
                            { p:rec.p, l:rec.l, c:now },
                            { $set:{ s:newShortKey }},
                            { w:1 },
                            function (err) {
                                if (err) return callback (err);
                                rec.s = newShortKey;
                                callback (undefined, rec);
                            }
                        );
                    }
                );
            }
        );

    setTimeout (function(){
        self.minsCollection.findOne ({ p:rec.p, l:rec.l }, function (err, rec) {
            if (err) return callback (err);
            if (!rec) return callback (new CompressionError (
                'UNSTABLE',
                'object key compression record suddenly disappeared'
            ));
            if (rec.s) return callback (undefined, rec);
            self.waitForShort (rec, callback);
        });
    }, this.config.collisionPoll);
};

/**     @property/Function compressOperatorLevel
    @development
    @private

*/
Compressor.prototype.compressOperatorLevel = function (rec, longpath, shortpath, parent, callback) {
    var self = this;
    try {
        var keys = Object.keys (rec);
    } catch (err) {
        return process.nextTick (function(){
            callback (undefined, rec);
        });
    }
    var compressed = {};
    async.each (keys, function (key, callback) {
        if (key == '_id') {
            compressed._id = rec._id;
            return callback();
        }
        var val = rec[key];
        var type = getTypeStr (val);

        if (key[0] == '$') {
            // operator!
            if (type != 'object') {
                compressed[key] = val;
                return callback();
            }
            return self.compressOperatorLevel (val, longpath, shortpath, parent, function (err, sublevel) {
                if (err) return callback (err);
                compressed[key] = sublevel;
                callback();
            });
        }

        var keyfrags = key.split ('.');

        var fullpath = longpath || '';
        var finalPath = '';
        var localParent = parent;
        for (var i in keyfrags) {
            var frag = keyfrags[i];
            var cached = self.minificationCache.get ('L', fullpath);
            if (cached) {
                finalPath += finalPath ? '.' + cached.s : cached.s;
                fullpath += '.' + frag;
                localParent = cached;
                continue;
            }

            // we don't have that cached, ask the database
            var remaining = keyfrags.slice (i);
            var rkeys = Object.keys (remaining);
            var DONE = {}; // just a token
            return async.eachSeries (rkeys, function (stepI, callback) {
                var step = remaining[stepI];

                self.minsCollection.findOne ({ p:localParent._id, l:step }, function (err, rec) {
                    if (err) return callback (err);
                    if (rec) {
                        if (!rec.s)
                            return self.waitForShort (rec, function (err, rec) {
                                if (err) return callback (err);
                                finalPath += finalPath ? '.' + rec.s : rec.s;
                                fullpath += '.' + step;
                                var newKeyDoc = {
                                    _id:    rec._id,
                                    L:      fullpath,
                                    l:      step,
                                    S:      shortpath + '.' + finalPath,
                                    s:      rec.s
                                };
                                self.minificationCache.set (newKeyDoc);
                                localParent = rec;
                                callback();
                            });

                        finalPath += finalPath ? '.' + rec.s : rec.s;
                        fullpath += '.' + step;
                        var newKeyDoc = {
                            _id:    rec._id,
                            L:      fullpath,
                            l:      step,
                            S:      shortpath + '.' + finalPath,
                            s:      rec.s
                        };
                        self.minificationCache.set (newKeyDoc);
                        localParent = rec;
                        return callback();
                    }

                    // novel key - insert new key records for each step
                    async.eachSeries (remaining.slice (stepI), function doStep (newKey, callback) {
                        fullpath += '.' + newKey;
                        uid.craft (function (newID) {
                            self.minsCollection.insert (
                                { _id:newID, p:localParent._id, l:newKey, c:(new Date()).getTime() },
                                { w:1 },
                                function (err) {
                                    if (!err) // successful novel key insertion
                                        return self.minsCollection.findAndModify (
                                            { _id:localParent._id },
                                            { _id:1 },
                                            { $inc:{ '%':1 }},
                                            { new:true },
                                            function (err, rec) {
                                                if (err) return callback (err);
                                                var newShortKey = getMin (rec['%']);
                                                self.minsCollection.update (
                                                    { _id:newID },
                                                    { $set:{ s:newShortKey }},
                                                    { w:1 },
                                                    function (err) {
                                                        if (err) return callback (err);
                                                        finalPath += finalPath ?
                                                            '.' + newShortKey
                                                          : newShortKey
                                                          ;
                                                        localParent = {
                                                            _id:    newID,
                                                            p:      localParent._id,
                                                            l:      newKey,
                                                            s:      newShortKey
                                                        };
                                                        var newKeyDoc = {
                                                            _id:    rec._id,
                                                            L:      fullpath,
                                                            l:      frag,
                                                            S:      shortpath + '.' + finalPath,
                                                            s:      newShortKey
                                                        };
                                                        self.minificationCache.set (newKeyDoc);
                                                        callback();
                                                    }
                                                );
                                            });

                                    if (err.code !== 11000) // unexpected error
                                        return callback (err);

                                    // collision!
                                    return self.minsCollection.findOne (
                                        { p:localParent._id, l:newKey },
                                        function (err, rec) {
                                            if (err)
                                                return callback (err);
                                            if (!rec)
                                                return callback (new CompressionError (
                                                    'UNSTABLE',
                                                    'object key compression record suddenly disappeared'
                                                ));

                                            if ( // is this somebody's failed key creation attempt?
                                                (new Date()).getTime() - rec.c
                                                    >
                                                self.config.collisionTimeout
                                            )
                                                return self.minsCollection.remove (
                                                    { p:localParent._id, l:newKey },
                                                    { w:1 },
                                                    function (err) {
                                                        if (err) return callback (err);
                                                        // try the key again
                                                        doStep (newKey, callback);
                                                    }
                                                );

                                            localParent = rec;
                                            if (rec.s) {
                                                finalPath += finalPath ? '.' + rec.s : rec.s;
                                                var newKeyDoc = {
                                                    _id:    rec._id,
                                                    L:      fullpath,
                                                    l:      frag,
                                                    S:      shortpath + '.' + finalPath,
                                                    s:      rec.s
                                                };
                                                self.minificationCache.set (newKeyDoc);
                                                return callback();
                                            }
                                            return self.waitForShort (rec, function (err, rec) {
                                                if (err) return callback (err);
                                                finalPath += finalPath ? '.' + rec.s : rec.s;
                                                var newKeyDoc = {
                                                    _id:    rec._id,
                                                    L:      fullpath,
                                                    l:      frag,
                                                    S:      shortpath + '.' + finalPath,
                                                    s:      rec.s
                                                };
                                                self.minificationCache.set (newKeyDoc);
                                                localParent = rec;
                                                callback();
                                            });
                                        }
                                    );
                                }
                            );
                        });
                    }, function (err) {
                        if (err) return callback (err);
                        callback (DONE);
                    });
                });
            }, function (err) {
                // the shortened path has been found or generated
                if (err && err !== DONE) return callback (err);

                var fullShortPath = shortpath + '.' + finalPath;
                if (type == 'object')
                    return self.compressOperatorLevel (
                        val,
                        fullpath,
                        fullShortPath,
                        localParent,
                        function (err, sublevel) {
                            if (err) return callback (err);
                            compressed[finalPath] = sublevel;
                            callback();
                        }
                    );
                else if (type == 'array')
                    return compressDocumentArray (
                        val,
                        fullpath,
                        fullShortPath,
                        localParent,
                        function (err, sublevel) {
                            if (err) return callback (err);
                            compressed[finalPath] = sublevel;
                            callback();
                        }
                    );

                compressed[finalPath] = val;
                callback();
            });
        }

        // the shortened path has been found or generated (synchronously)
        var fullShortPath = shortpath + '.' + finalPath;
        if (type == 'object')
            return self.compressOperatorLevel (val, fullpath, fullShortPath, localParent, function (err, sublevel) {
                if (err) return callback (err);
                compressed[finalPath] = sublevel;
                callback();
            });
        else if (type == 'array')
            return compressArray (val, fullpath, fullShortPath, localParent, function (err, sublevel) {
                if (err) return callback (err);
                compressed[finalPath] = sublevel;
                callback();
            });

        compressed[finalPath] = val;
        callback();
    }, function (err) {
        callback (err, compressed);
    });
}


/**     @property/Function compressOperatorArray
    @development
    @private

*/
Compressor.prototype.compressOperatorArray = function (arr, longpath, shortpath, parent, callback) {
    var self = this;
    var newArray = [];
    var indexes = Object.keys (arr);
    async.each (indexes, function (i, callback) {
        var val = arr[i];
        var type = getTypeStr (val);

        if (type == 'object')
            return self.compressOperatorLevel (val, longpath, shortpath, parent, function (err, sublevel) {
                if (err) return callback (err);
                newArray[i] = sublevel;
                callback();
            });
        else if (type == 'array')
            return self.compressOperatorArray (
                val,
                longpath,
                shortpath,
                parent,
                function (err, sublevel) {
                    if (err) return callback (err);
                    newArray[i] = sublevel;
                    callback();
                }
            );

        newArray[i] = val;
        callback();
    }, function (err) {
        callback (err, newArray);
    });
}


/**     @property/Function compressDocumentLevel
    @development
    @private

*/
Compressor.prototype.compressDocumentLevel = function (rec, longpath, shortpath, parent, callback) {
    var self = this;
    var keys = Object.keys (rec);
    var compressed = {};
    async.each (keys, function (key, callback) {
        if (key == '_id') {
            compressed._id = rec._id;
            return callback();
        }

        var val = rec[key];
        var type = getTypeStr (val);

        var fullpath = longpath + '.' + key;
        var cached = self.minificationCache.get ('L', fullpath);
        if (cached) {
            if (type == 'object')
                return self.compressDocumentLevel (
                    val,
                    fullpath,
                    shortpath + '.' + cached.s,
                    cached,
                    function (err, subCompressed) {
                        if (err) return callback (err);
                        compressed[cached.s] = subCompressed;
                        callback();
                    }
                );
            if (type == 'array')
                return self.compressDocumentArray (
                    val,
                    fullpath,
                    shortpath + '.' + cached.s,
                    cached,
                    function (err, subCompressed) {
                        if (err) return callback (err);
                        compressed[cached.s] = subCompressed;
                        callback();
                    }
                );

            compressed[cached.s] = val;
            return callback();
        }

        // we don't have that cached, ask the database
        function finalCall (short, levelParent) {
            if (type == 'object')
                return self.compressDocumentLevel (
                    val,
                    fullpath,
                    shortpath + '.' + short,
                    levelParent,
                    function (err, subCompressed) {
                        if (err) return callback (err);
                        compressed[short] = subCompressed;
                        callback();
                    }
                );
            if (type == 'array')
                return self.compressDocumentArray (
                    val,
                    fullpath,
                    shortpath + '.' + short,
                    levelParent,
                    function (err, subCompressed) {
                        if (err) return callback (err);
                        compressed[short] = subCompressed;
                        callback();
                    }
                );

            compressed[short] = val;
            callback();
        }

        self.minsCollection.findOne ({ p:parent._id, l:key }, function (err, rec) {
            if (err) return callback (err);
            if (rec) {
                // already established
                if (!rec.s) {
                    return self.waitForShort (rec, function (err, rec) {
                        if (err) return callback (err);
                        var newKeyDoc = {
                            _id:    rec._id,
                            L:      fullpath,
                            l:      key,
                            S:      shortpath + '.' + rec.s,
                            s:      rec.s
                        };
                        self.minificationCache.set (newKeyDoc);
                        finalCall (rec.s, newKeyDoc);
                    });
                }

                var newKeyDoc = {
                    _id:    rec._id,
                    L:      fullpath,
                    l:      key,
                    S:      shortpath + '.' + rec.s,
                    s:      rec.s
                };
                self.minificationCache.set (newKeyDoc);
                return finalCall (rec.s, newKeyDoc);
            }

            // novel key - insert new key record
            uid.craft (function (newID) {
                self.minsCollection.insert (
                    { _id:newID, p:parent._id, l:key, c:(new Date()).getTime() },
                    { w:1 },
                    function (err) {
                        if (!err) // successful novel key insertion
                            return self.minsCollection.findAndModify (
                                { _id:parent._id },
                                { _id:1 },
                                { $inc:{ '%':1 }},
                                { new:true },
                                function (err, rec) {
                                    if (err) return callback (err);
                                    var newShortKey = getMin (rec['%']);
                                    self.minsCollection.update (
                                        { _id:newID },
                                        { $set:{ s:newShortKey }},
                                        { w:1 },
                                        function (err) {
                                            if (err) return callback (err);
                                            var newKeyDoc = {
                                                _id:    newID,
                                                L:      fullpath,
                                                l:      key,
                                                S:      shortpath + '.' + newShortKey,
                                                s:      newShortKey
                                            };
                                            self.minificationCache.set (newKeyDoc);
                                            return finalCall (newShortKey, newKeyDoc);
                                        }
                                    );
                                }
                            );

                        if (err.code !== 11000) // unexpected error
                            return callback (err);

                        // collision!
                        return self.minsCollection.findOne (
                            { p:parent._id, l:key },
                            function (err, rec) {
                                if (err)
                                    return callback (err);
                                if (!rec)
                                    return callback (new Error (
                                        'object key compression record suddenly disappeared'
                                    ));

                                if ( // is this somebody's failed key creation attempt?
                                    (new Date()).getTime() - rec.c
                                        >
                                    self.config.collisionTimeout
                                )
                                    return self.minsCollection.remove (
                                        { p:parent._id, l:key },
                                        { w:1 },
                                        function (err) {
                                            if (err) return callback (err);
                                            // try the key again
                                            doStep (key, callback);
                                        }
                                    );

                                if (rec.s) {
                                    var newKeyDoc = {
                                        _id:    rec._id,
                                        L:      fullpath,
                                        l:      key,
                                        S:      shortpath + '.' + rec.s,
                                        s:      rec.s
                                    };
                                    self.minificationCache.set (newKeyDoc);
                                    return finalCall (rec.s, newKeyDoc);
                                }

                                self.waitForShort (rec, function (err, rec) {
                                    if (err) return callback (err);
                                    var newKeyDoc = {
                                        _id:    rec._id,
                                        L:      fullpath,
                                        l:      key,
                                        S:      shortpath + '.' + rec.s,
                                        s:      rec.s
                                    };
                                    self.minificationCache.set (newKeyDoc);
                                    return finalCall (rec.s, newKeyDoc);
                                });
                            }
                        );
                    }
                );
            });
        });
    }, function (err) {
        callback (err, compressed);
    });
}


/**     @property/Function compressDocumentArray
    @development
    @private

*/
Compressor.prototype.compressDocumentArray = function (arr, longpath, shortpath, parent, callback) {
    var self = this;
    var newArray = [];
    var indexes = Object.keys (arr);
    async.each (indexes, function (i, callback) {
        var val = arr[i];
        var type = getTypeStr (val);

        if (type == 'object')
            return self.compressDocumentLevel (val, longpath, shortpath, parent, function (err, sublevel) {
                if (err) return callback (err);
                newArray[i] = sublevel;
                callback();
            });
        else if (type == 'array')
            return compressDocumentArray (
                val,
                longpath,
                shortpath,
                parent,
                function (err, sublevel) {
                    if (err) return callback (err);
                    newArray[i] = sublevel;
                    callback();
                }
            );

        newArray[i] = val;
        callback();
    }, function (err) {
        callback (err, newArray);
    });
}


/**     @property/Function compressPath

*/
Compressor.prototype.compressPath = function (root, path, callback) {
    if (arguments.length == 2) {
        callback = path;
        path = root;
        root = this.documentClass;
    }

    var self = this;
    var pathFrags = path.split ('.');
    var fullpath = root._id;
    var shortpath = root._id;
    var compressedPath = '';
    var parent = root;
    for (var i in pathFrags) {
        var frag = pathFrags[i];
        fullpath += '.' + frag;
        var cached = self.minificationCache.get ('L', fullpath);
        if (cached) {
            compressedPath += compressedPath ? '.' + cached.s : cached.s;
            shortpath += cached.s;
            parent = cached;
            continue;
        }

        // we don't have that cached, turn to the database
        var remaining = pathFrags.slice (i);
        var rkeys = Object.keys (remaining);
        var DONE = {}; // just a token
        return async.eachSeries (rkeys, function (stepI, callback) {
            var step = remaining[stepI];

            self.minsCollection.findOne ({ p:parent._id, l:step }, function (err, rec) {
                if (err) return callback (err);
                if (!rec)
                    return callback (new Error ('cannot resolve path '+fullpath));

                if (!rec.s)
                    return self.waitForShort (rec, function (err, rec) {
                        if (err) return callback (err);
                        compressedPath += compressedPath ? '.' + rec.s : rec.s;
                        shortpath += '.' + rec.s;
                        var newKeyDoc = {
                            _id:    rec._id,
                            L:      fullpath,
                            l:      frag,
                            S:      shortpath,
                            s:      rec.s
                        };
                        self.minificationCache.set (newKeyDoc);
                        parent = rec;
                        callback();
                    });

                compressedPath += compressedPath ? '.' + rec.s : rec.s;
                shortpath += '.' + rec.s;
                var newKeyDoc = {
                    _id:    rec._id,
                    L:      fullpath,
                    l:      frag,
                    S:      shortpath,
                    s:      rec.s
                };
                self.minificationCache.set (newKeyDoc);
                parent = rec;
                callback();
            });
        }, function (err) {
            callback (err, compressedPath, parent);
        });
    }

    process.nextTick (function(){ callback (undefined, compressedPath, parent); });
}


/**     @local/Function compressAndEnsurePath

*/
Compressor.prototype.compressAndEnsurePath = function (root, path, callback) {
    var self = this;
    var pathFrags = path.split ('.');
    var fullpath = root._id;
    var shortpath = root._id;
    var compressedPath = '';
    var parent = root;
    for (var i in pathFrags) {
        var frag = pathFrags[i];
        fullpath += '.' + frag;
        var cached = self.minificationCache.get ('L', fullpath);
        if (cached) {
            compressedPath += compressedPath ? '.' + cached.s : cached.s;
            shortpath += cached.s;
            parent = cached;
            continue;
        }

        // we don't have that cached, turn to the database
        var first = true;
        return async.eachSeries (pathFrags.slice (i), function doStep (step, callback) {
            if (first)
                first = false;
            else
                fullpath += '.' + step;
            self.minsCollection.findOne ({ p:parent._id, l:step }, function (err, rec) {
                if (err) return callback (err);

                if (rec) {
                    if (!rec.s)
                        return self.waitForShort (rec, function (err, rec) {
                            if (err) return callback (err);
                            compressedPath += compressedPath ? '.' + rec.s : rec.s;
                            shortpath += '.' + rec.s;
                            var newKeyDoc = {
                                _id:    rec._id,
                                L:      fullpath,
                                l:      frag,
                                S:      shortpath,
                                s:      rec.s
                            };
                            self.minificationCache.set (newKeyDoc);
                            parent = rec;
                            callback();
                        });

                    compressedPath += compressedPath ? '.' + rec.s : rec.s;
                    shortpath += '.' + rec.s;
                    var newKeyDoc = {
                        _id:    rec._id,
                        L:      fullpath,
                        l:      frag,
                        S:      shortpath,
                        s:      rec.s
                    };
                    self.minificationCache.set (newKeyDoc);
                    parent = rec;
                    return callback();
                }

                // minification not found
                // novel key - insert new key record
                uid.craft (function (newID) {
                    self.minsCollection.insert (
                        { _id:newID, p:parent._id, l:step, c:(new Date()).getTime() },
                        { w:1 },
                        function (err) {
                            if (!err) // successful novel key insertion
                                return self.minsCollection.findAndModify (
                                    { _id:parent._id },
                                    { _id:1 },
                                    { $inc:{ '%':1 }},
                                    { new:true },
                                    function (err, rec) {
                                        if (err) return callback (err);
                                        var newShortKey = getMin (rec['%']);
                                        self.minsCollection.update (
                                            { _id:newID },
                                            { $set:{ s:newShortKey }},
                                            { w:1 },
                                            function (err) {
                                                if (err) return callback (err);
                                                shortpath += '.' + newShortKey;
                                                var newKeyDoc = {
                                                    _id:    newID,
                                                    L:      fullpath,
                                                    l:      step,
                                                    S:      shortpath,
                                                    s:      newShortKey
                                                };
                                                self.minificationCache.set (newKeyDoc);
                                                parent = newKeyDoc;
                                                compressedPath += compressedPath ?
                                                    '.' + newShortKey
                                                  : newShortKey
                                                  ;
                                                callback();
                                            }
                                        );
                                    }
                                );

                            if (err.code !== 11000) // unexpected error
                                return callback (err);

                            // collision!
                            return self.minsCollection.findOne (
                                { p:parent._id, l:step },
                                function (err, rec) {
                                    if (err)
                                        return callback (err);
                                    if (!rec)
                                        return callback (new CompressionError (
                                            'UNSTABLE',
                                            'object key compression record suddenly disappeared'
                                        ));

                                    if ( // is this somebody's failed key creation attempt?
                                        (new Date()).getTime() - rec.c
                                            >
                                        self.config.collisionTimeout
                                    )
                                        return self.minsCollection.remove (
                                            { p:parent._id, l:step },
                                            { w:1 },
                                            function (err) {
                                                if (err) return callback (err);
                                                // try the key again
                                                doStep (step, callback);
                                            }
                                        );

                                    if (rec.s) {
                                        shortpath += '.' + rec.s;
                                        var newKeyDoc = {
                                            _id:    rec._id,
                                            L:      fullpath,
                                            l:      step,
                                            S:      shortpath,
                                            s:      rec.s
                                        };
                                        self.minificationCache.set (newKeyDoc);
                                        parent = newKeyDoc;
                                        compressedPath += compressedPath ?
                                            '.' + rec.s
                                          : rec.s
                                          ;
                                        return callback();
                                    }

                                    self.waitForShort (rec, function (err, rec) {
                                        if (err) return callback (err);
                                        shortpath += '.' + rec.s;
                                        var newKeyDoc = {
                                            _id:    rec._id,
                                            L:      fullpath,
                                            l:      step,
                                            S:      shortpath,
                                            s:      rec.s
                                        };
                                        self.minificationCache.set (newKeyDoc);
                                        parent = newKeyDoc;
                                        compressedPath += compressedPath ?
                                            '.' + rec.s
                                          : rec.s
                                          ;
                                        callback();
                                    });
                                }
                            );
                        }
                    );
                });
            });
        }, function (err) {
            callback (err, compressedPath, parent);
        });
    }

    process.nextTick (function(){ callback (undefined, compressedPath, parent); });
}


/**     @property/Function decompressPath

*/
Compressor.prototype.decompressPath = function (root, path, callback) {
    if (arguments.length == 2) {
        callback = path;
        path = root;
        root = this.documentClass;
    }
    var self = this;
    var pathFrags = path.split ('.');
    var fullpath = root._id;
    var shortpath = root._id;
    var decompressedPath = '';
    var parent = root;
    for (var i in pathFrags) {
        var frag = pathFrags[i];
        shortpath += '.' + frag;
        var cached = self.minificationCache.get ('S', shortpath);
        if (cached) {
            decompressedPath += decompressedPath ? '.' + cached.l : cached.l;
            fullpath += cached.l;
            parent = cached;
            continue;
        }

        // we don't have that cached, turn to the database
        var remaining = pathFrags.slice (i);
        var rkeys = Object.keys (remaining);
        var DONE = {}; // just a token
        return async.eachSeries (rkeys, function (stepI, callback) {
            var step = remaining[stepI];

            self.minsCollection.findOne ({ p:parent._id, s:step }, function (err, rec) {
                if (err) return callback (err);
                if (!rec) return callback (new Error ('cannot resolve path '+shortpath));

                decompressedPath += decompressedPath ? '.' + rec.l : rec.l;
                fullpath += rec.l;
                shortpath += '.' + rec.s;
                var newKeyDoc = {
                    _id:    rec._id,
                    L:      fullpath,
                    l:      frag,
                    S:      shortpath,
                    s:      rec.s
                };
                self.minificationCache.set (newKeyDoc);
                parent = rec;
                callback();
            });
        }, function (err) {
            callback (err, decompressedPath, parent);
        });
    }

    process.nextTick (function(){ callback (undefined, decompressedPath, parent); });
}


/**     @property/Function self.decompressLevel
    @development
    @private

*/
Compressor.prototype.decompressLevel = function (rec, shortpath, longpath, parent, callback) {
    var self = this;
    var keys = Object.keys (rec);
    var decompressed = {};

    async.each (keys, function (key, callback) {
        if (key == '_id') {
            decompressed._id = rec._id;
            return callback();
        }

        var val = rec[key];
        var type = getTypeStr (val);

        var fullShortPath = shortpath + '.' + key;
        var cached = self.minificationCache.get ('S', fullShortPath);
        if (cached) {
            if (type == 'object')
                return self.decompressLevel (
                    val,
                    fullShortPath,
                    longpath + '.' + cached.l,
                    cached,
                    function (err, sublevel) {
                        if (err) return callback (err);
                        decompressed[cached.l] = sublevel;
                        callback();
                    }
                );
            else if (type == 'array')
                return self.decompressArray (
                    val,
                    fullShortPath,
                    longpath + '.' + cached.l,
                    cached,
                    function (err, sublevel) {
                        if (err) return callback (err);
                        decompressed[cached.l] = sublevel;
                        callback();
                    }
                );

            decompressed[cached.l] = val;
            return callback();
        }

        // not cached, ask the database
        self.minsCollection.findOne ({ p:parent._id, s:key }, function (err, rec) {
            if (err) return callback (err);
            if (!rec)
                return callback (new Error ('compressed document cannot be recovered'));
            var fullShortPath = shortpath + '.' + key;
            var newKeyDoc = {
                _id:    rec._id,
                S:      fullShortPath,
                s:      rec.s,
                L:      longpath + '.' + rec.l,
                l:      rec.l
            };
            self.minificationCache.set (newKeyDoc);

            if (type == 'object')
                return self.decompressLevel (
                    val,
                    fullShortPath,
                    longpath + '.' + rec.l,
                    newKeyDoc,
                    function (err, sublevel) {
                        if (err) return callback (err);
                        decompressed[rec.l] = sublevel;
                        callback();
                    }
                );
            else if (type == 'array')
                return self.decompressArray (
                    val,
                    fullShortPath,
                    longpath + '.' + rec.l,
                    newKeyDoc,
                    function (err, sublevel) {
                        if (err) return callback (err);
                        decompressed[rec.l] = sublevel;
                        callback();
                    }
                );

            decompressed[rec.l] = val;
            return callback();
        });

    }, function (err) {
        callback (err, decompressed);
    });
}


/**     @property/Function self.decompressArray
    @development
    @private

*/
Compressor.prototype.decompressArray = function (arr, shortpath, longpath, parent, callback) {
    var self = this;
    var newArray = [];
    var indexes = Object.keys (arr);
    async.each (indexes, function (i, callback) {
        var val = arr[i];
        var type = getTypeStr (val);
        if (type == 'array')
            return self.decompressArray (
                val,
                shortpath,
                longpath,
                parent,
                function (err, decompressedElem) {
                    if (err) return callback (err);
                    newArray[i] = decompressedElem;
                    callback();
                }
            );
        if (type == 'object')
            return self.decompressLevel (
                val,
                shortpath,
                longpath,
                parent,
                function (err, decompressedElem) {
                    if (err) return callback (err);
                    newArray[i] = decompressedElem;
                    callback();
                }
            );

        newArray[i] = val;
        callback();
    }, function (err) {
        callback (err, newArray);
    });
}


/**     @property/Function compressRecord
    @api

@argument/Object record
@callback
    @argument/Error|undefined err
    @argument/Object|undefined compressed
*/
Compressor.prototype.compressRecord = function (rec, callback) {
    var self = this;

    this.compressDocumentLevel (
        rec,
        self.documentClass._id,
        self.documentClass._id,
        self.documentClass,
        callback
    );
}


/**     @property/Function compressQuery
    @api

@argument/Object query
@callback
    @argument/Error|undefined err
    @argument/Object|undefined compressed
*/
Compressor.prototype.compressQuery = function (query, callback) {
    var self = this;

    this.compressOperatorLevel (
        query,
        self.documentClass._id,
        self.documentClass._id,
        self.documentClass,
        callback
    );
}


/**     @property/Function compressProjection
    @api

@argument/Object projection
@callback
    @argument/Error|undefined err
    @argument/Object|undefined compressed
*/
Compressor.prototype.compressProjection = function (projection, callback) {
    this.compressOperatorLevel (
        projection,
        this.documentClass._id,
        this.documentClass._id,
        this.documentClass,
        callback
    );
}


/**     @property/Function compressUpdate
    @api

@argument/Object update
@callback
    @argument/Error|undefined err
    @argument/Object|undefined compressed
*/
Compressor.prototype.compressUpdate = function (update, callback) {
    this.compressOperatorLevel (
        update,
        this.documentClass._id,
        this.documentClass._id,
        this.documentClass,
        callback
    );
}


/**     @property/Function compressSort

@argument/Object|Array sort
@callback
    @argument/Error|undefined err
    @argument/Object|Array compressedSort
*/
Compressor.prototype.compressSort = function (sort, callback) {
    var self = this;

    if (getTypeStr (sort) == 'array') {
        var compressed = [];
        return async.each (Object.keys (sort), function (termI, callback) {
            self.compressPath (
                self.documentClass,
                sort[termI][0],
                function (err, compressedTerm) {
                    if (err) return callback (err);
                    compressed[termI] = [ compressedTerm, sort[termI][1] ];
                    callback();
                }
            );
        }, function (err) {
            callback (err, compressed);
        });
    }

    var keys = Object.keys (sort);
    if (keys.length != 1)
        throw new Error ('Object sort definitions must contain exactly one key');
    return this.compressPath (self.documentClass, keys[0], function (err, compressedTerm) {
        if (err) return callback (err);
        var newSort = {};
        newSort[compressedTerm] = sort[keys[0]];
        callback (undefined, newSort);
    });
}


/**     @member/Function compressIndexSpec

*/
Compressor.prototype.compressIndexSpec = function (spec, callback) {
    var self = this;
    var paths = Object.keys (spec);
    var compressedIndex = {};
    return async.eachSeries (paths, function (path, callback) {
        self.compressAndEnsurePath (
            self.documentClass,
            path,
            function (err, compressedPath) {
                if (err) return callback (err);
                compressedIndex[compressedPath] = spec[path];
                callback();
            }
        );
    }, function (err) {
        if (err) return callback (err);
        callback (undefined, compressedIndex);
    });
}


/**     @member/Function compressIndexName

*/
Compressor.prototype.compressIndexName = function (name, callback) {
    var indexFrags = name.split ('_');
    var fauxDoc = {};
    for (var i=0,j=indexFrags.length; i<j; i+=2)
        fauxDoc[indexFrags[i]] = indexFrags[i+1];
    this.compressQuery (fauxDoc, function (err, fauxIndex) {
        if (err) return callback (err);

        var outfrags = [];
        for (var key in fauxIndex) {
            outfrags.push (key);
            outfrags.push (fauxIndex[key]);
        }
        callback (undefined, outfrags.join ('_'));
    });
};


/**     @member/Function decompressIndexName

*/
Compressor.prototype.decompressIndexName = function (name, callback) {
    var indexFrags = name.split ('_');
    var fauxDoc = {};
    for (var i=0,j=indexFrags.length; i<j; i+=2) {
        var keyFrags = indexFrags[i].split('.');
        var pointer = fauxDoc;
        for (var k=0,l=keyFrags.length-1; k<l; k++)
            pointer = pointer[keyFrags[k]] = {};
        pointer[keyFrags[keyFrags.length-1]] = indexFrags[i+1];
        continue;
    }
    this.decompress (fauxDoc, function (err, fauxIndex) {
        if (err) return callback (err);

        var outfrags = [];
        for (var key in fauxIndex) {
            outfrags.push (key);
            outfrags.push (fauxIndex[key]);
        }
        callback (undefined, outfrags.join ('_'));
    });
};


/**     @member/Function compressAggregationPipeline

@argument/Array[Object] pipeline
@callback
    @argument/Error|undefined err
    @argument/Array[Object]|undefined compressedPipeline
*/
Compressor.prototype.compressAggregationPipeline = function (pipe, callback) {
    var self = this;
    var compressedPipe = [];
    async.times (pipe.length, function (stageI, callback) {
        var stage = pipe[stageI];
        var operator;
        try {
            operator = Object.keys (stage)[0];
        } catch (err) {
            // bogus stage, fail
            return callback (err);
        }
        var expression = stage[operator];

        if (operator == '$geoNear') {
            if (stageI)
                return callback (new Error ('$geoNear must be the first stage'));
            if (typeof expression != 'object')
                return callback (new Error ('invalid $geoNear stage'));

            var jobs = [];
            var newExpression = {};
            for (var key in expression) newExpression[key] = expression[key];

            if (expression.distanceField)
                jobs.push (function (callback) {
                    self.compressAndEnsurePath (
                        self.documentClass,
                        expression.distanceField,
                        function (err, compressedPath) {
                            if (err) return callback (err);
                            newExpression.distanceField = compressedPath;
                            callback();
                        }
                    );
                });

            if (expression.includeLocs)
                jobs.push (function (callback) {
                    self.compressAndEnsurePath (
                        self.documentClass,
                        expression.includeLocs,
                        function (err, compressedPath) {
                            if (err) return callback (err);
                            newExpression.includeLocs = compressedPath;
                            callback();
                        }
                    );
                });

            if (expression.query)
                jobs.push (function (callback) {
                    self.compressQuery (expression.query, function (err, compressedQuery) {
                        if (err) return callback (err);
                        newExpression.query = compressedQuery;
                        callback();
                    });
                });

            return async.parallel (jobs, function (err) {
                if (err) return callback (err);
                compressedPipe[stageI] = { $geoNear:newExpression };
                callback();
            });
        }

        if (operator == '$group') {
            if (typeof expression != 'object')
                return callback (new Error ('invalid $group stage'));

            var paths = Object.keys (expression);
            var compressedSpec = {};
            return async.each (paths, function (path, callback) {
                if (path == '_id') {
                    compressedPath = path;
                    return self.compressExpression (
                        expression._id,
                        function (err, compressedExpression) {
                            if (err) return callback (err);
                            compressedSpec[compressedPath] = compressedExpression;
                            callback();
                        }
                    );
                }

                self.compressAndEnsurePath (
                    self.documentClass,
                    path,
                    function (err, compressedPath, node) {
                        if (err) return callback (err);
                        self.compressExpression (
                            expression[path],
                            function (err, compressedExpression) {
                                if (err) return callback (err);
                                compressedSpec[compressedPath] = compressedExpression;
                                callback();
                        }, node);
                    }
                );
            }, function (err) {
                if (err) return callback (err);
                compressedPipe[stageI] = { $group:compressedSpec };
                callback();
            });
        }

        if (operator == '$match') {
            if (typeof expression != 'object')
                return callback (new Error ('invalid $match stage'));

            return self.compressQuery (expression, function (err, compressedQuery) {
                if (err) return callback();
                compressedPipe[stageI] = { $match:compressedQuery };
                callback();
            });
        }

        if (operator == '$project') {
            if (typeof expression != 'object')
                return callback (new Error ('invalid $projection stage'));

            var newSpec = {};
            return async.each (Object.keys (expression), function (key, callback) {
                self.compressAndEnsurePath (
                    self.documentClass,
                    key,
                    function (err, compressedPath, node) {
                        if (err) return callback (err);
                        self.compressExpression (
                            expression[key],
                            function (err, compressedExpression) {
                                if (err) return callback (err);
                                newSpec[compressedPath] = compressedExpression;
                                callback();
                            },
                            node
                        );
                    }
                );
            }, function (err) {
                if (err) return callback (err);
                compressedPipe[stageI] = { $project:newSpec };
                callback();
            });
        }

        if (operator == '$sort') {
            if (typeof expression != 'object')
                return callback (new Error ('invalid $sort stage'));

            return self.compressSort (expression, function (err, compressedSort) {
                if (err) return callback();
                compressedPipe[stageI] = { $sort:compressedSort };
                callback();
            });
        }

        if (operator == '$unwind') {
            if (typeof expression != 'string')
                return callback (new Error ('invalid $unwind stage'));

            return self.compressAndEnsurePath (
                self.documentClass,
                expression.slice (1),
                function (err, compressedPath) {
                    if (err) return callback();
                    compressedPipe[stageI] = { $unwind:'$'+compressedPath };
                    callback();
                }
            );
        }

        if (operator == '$redact') {
            if (typeof expression != 'object')
                return callback (new Error ('invalid $redact stage'));
            return self.compressExpression (expression, function (err, compressedExpression) {
                if (err) return callback (err);
                compressedPipe[stageI] = { $redact:compressedExpression };
                callback();
            });
        }

        // stage does not require compression
        compressedPipe[stageI] = stage
        callback();
    }, function (err) {
        callback (err, err ? undefined : compressedPipe);
    });
};


/**     @member/Function compressExpression

*/
Compressor.prototype.compressExpression = function (expression, callback, root) {
    var self = this;
    var type = getTypeStr (expression);

    if (type == 'object') {
        var expressionKeys = Object.keys (expression);
        var firstKey = expressionKeys[0];

        if (firstKey[0] != '$') {
            // literal Object
            var newExpression = {};
            var self = this;
            return async.each (expressionKeys, function (key, callback) {
                self.compressAndEnsurePath (
                    root || self.documentClass,
                    key,
                    function (err, compressedPath, node) {
                        if (err) return callback (err);
                        self.compressExpression (
                            expression[key],
                            function (err, compressedExpression) {
                                if (err) return callback (err);
                                newExpression[compressedPath] = compressedExpression;
                                callback();
                            },
                            node
                        );
                    }
                );
            }, function (err) {
                callback (err, err ? undefined : newExpression);
            });
        }

        // step the expression up to the useful section
        expression = expression[firstKey];

        if (firstKey == '$literal')
            return process.nextTick (function(){ callback (undefined, expression); });

        if (firstKey == '$let') {
            var self = this;
            var newExpression = { vars:{} };
            return async.parallel ([
                function (callback) {
                    self.compressExpression (expression.in, function (err, compressedExpression) {
                        if (err) return callback (err);
                        newExpression.in = compressedExpression;
                        callback();
                    }, root);
                },
                function (callback) {
                    async.each (Object.keys (expression.vars), function (varExpression, callback) {
                        self.compressExpression (
                            varExpression,
                            function (err, compressedExpression) {
                                if (err) return callback (err);
                                newExpression.vars[key] = compressedExpression;
                                callback();
                            },
                            root
                        );
                    });
                }
            ], function (err) {
                callback (err, err ? undefined : { $let:newExpression });
            });
        }

        if (firstKey == '$map') {
            var newExpression = { as:expression.as };
            return async.parallel ([
                function (callback) {
                    self.compressExpression (expression.input, function (err, compressedExpression) {
                        if (err) return callback (err);
                        newExpression.input = compressedExpression;
                        callback();
                    }, root);
                },
                function (callback) {
                    self.compressExpression (expression.in, function (err, compressedExpression) {
                        if (err) return callback (err);
                        newExpression.in = compressedExpression;
                        callback();
                    }, root);
                }
            ], function (err) {
                callback (err, err ? undefined : { $map:newExpression });
            });
        }

        if (firstKey == '$cond') {
            var newExpression = {};
            return async.parallel ([
                function (callback) {
                    self.compressExpression (expression.if, function (err, compressedExpression) {
                        if (err) return callback (err);
                        newExpression.if = compressedExpression;
                        callback();
                    }, root);
                },
                function (callback) {
                    self.compressExpression (expression.then, function (err, compressedExpression) {
                        if (err) return callback (err);
                        newExpression.then = compressedExpression;
                        callback();
                    }, root);
                },
                function (callback) {
                    self.compressExpression (expression.else, function (err, compressedExpression) {
                        if (err) return callback (err);
                        newExpression.else = compressedExpression;
                        callback();
                    }, root);
                }
            ], function (err) {
                callback (err, err ? undefined : { $cond:newExpression });
            });
        }

        // process a single expression operator
        return this.compressExpression (expression, function (err, compressedExpression) {
            if (err) return callback (err);
            var newExpression = {};
            newExpression[firstKey] = compressedExpression;
            callback (undefined, newExpression);
        }, root);
    }

    if (type == 'array') {
        var newExpression = [];
        var self = this;
        return async.times (expression.length, function (i, callback) {
            self.compressExpression (expression[i], function (err, compressedExpression) {
                if (err) return callback (err);
                newExpression[i] = compressedExpression;
                callback();
            }, root);
        }, function (err) {
            callback (err, err ? undefined : newExpression);
        });
    }

    if (type == 'string') {
        if (expression[0] != '$' || expression[1] == '$')
            return process.nextTick (function(){ callback (undefined, expression); });
        return this.compressAndEnsurePath (
            this.documentClass,
            expression.slice (1),
            function (err, compressedPath) {
                callback (err, err ? undefined : ('$' + compressedPath));
            }
        );
    }

    // literal
    return process.nextTick (function(){ callback (undefined, expression); });
};


/**     @member/Function decompress
    @api
    Decompress a previously [compressed](#compressRecord) database record.
@argument/Object compressed
@callback
    @argument/Error|undefined err
    @argument/Object|undefined record
*/
Compressor.prototype.decompress = function (root, compressed, callback) {
    if (arguments.length == 2) {
        callback = compressed;
        compressed = root;
        root = undefined;
    }

    if (!root)
        return this.decompressLevel (
            compressed,
            this.documentClass._id,
            this.documentClass._id,
            this.documentClass,
            callback
        );

    // check if the entire root path is already cached
    var fullroot = this.documentClass + '.' + root;
    var cached = this.minificationCache.get ('L', fullroot);
    if (cached) {
        return this.decompressLevel (
            compressed,
            cached.S,
            cached.L,
            cached,
            callback
        );
    }

    // start with the document root and step up to the path, then decompress
    var self = this;
    return this.compressPath (
        self.documentClass,
        root,
        function (err, shortpath, parent) {
            if (err) return callback (err);
            self.decompressLevel (
                compressed,
                parent.S,
                parent.L,
                parent,
                callback
            );
        }
    );
}


/**     @property/Function getSchemaInfo
    @api

@argument/String path
@callback
    @argument/Error|undefined err
    @argument/Object|undefined info
*/
Compressor.prototype.getSchemaInfo = function (path, callback) {
    var self = this;

}


/**     @property/Function setSchemaInfo
    @api

@argument/String path
@argument/Object info
@callback
    @argument/Error|undefined err
*/
Compressor.prototype.setSchemaInfo = function (path, info, callback) {
    var self = this;

}


/**     @property/Function getChildKeys
    @api

@argument/String path
@callback
    @argument/Error|undefined err
    @argument/Array[String] keys
*/
Compressor.prototype.getChildKeys = function (path, callback) {
    var self = this;

}

module.exports = Compressor;
