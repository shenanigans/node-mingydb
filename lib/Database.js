
/**     @module waxwork     */

var Collection  = require ('./Collection');
var MongoDB     = require ('mongodb');
var async       = require ('async');

/**     @class Database

*/
function Database (dbName, mongoloid) {
    this.name = dbName;
    this.mongoloid = mongoloid;
}
module.exports = Database;
var Compressor  = require ('./Compressor');


/**     @member/Function Database#close

*/
Database.prototype.close = function (callback) {
    return this.mongoloid.close (callback);
}


/**     @member/Function Database#db

*/
Database.prototype.db = function (dbName) {
    return new Database (dbName, this.mongoloid.db (dbName));
}


/**     @member/Function Database#admin

*/
Database.prototype.admin = function (callback) {
    return this.mongoloid.admin (callback);
}


/**     @member/Function Database#listCollections

*/
Database.prototype.listCollections = function(){
    return this.mongoloid.listCollections.apply (this.mongoloid, arguments);
}


/**     @member/Function Database#collectionNames

*/
Database.prototype.collectionNames = function (callback) {
    return this.mongoloid.collectionNames (function (err, info) {
        if (err) return callback (err);
        var output = [];
        for (var i in info)
            output.push (info[i].name.replace (/^[^.]*\./, ''));
        callback (undefined, output);
    });
}


/**     @member/Function Database#collection

*/
Database.prototype.collection = function (collectionName, callback) {
    var self = this;
    return this.mongoloid.collection (collectionName, function (err, col) {
        if (err) return callback (err);
        var compressor = new Compressor (self.name, collectionName);
        compressor.ready (function (err) {
            if (err) return callback (err);
            callback (undefined, new Collection (collectionName, col, compressor));
        });
    });
};


/**     @member/Function Database#collections

*/
Database.prototype.collections = function (callback) {
    var self = this;
    return this.collectionNames (function (err, colNames) {
        if (err) return callback (err);
        var output = [];
        async.each (colNames, function (collectionName, callback) {
            self.mongoloid.collection (collectionName, function (err, col) {
                if (err) return callback (err);
                var compressor = new Compressor (self.name, collectionName);
                compressor.ready (function (err) {
                    if (err) return callback (err);
                    output.push (new Collection (collectionName, collectionName, col, compressor));
                    callback();
                });
            });
        }, function (err) {
            callback (err, err ? undefined : output);
        });
    });
};


/**     @property/Function getDatabase
    Return a wrapped database [instance](waxwork.Database) instance with inline [compression]
    (waxwork.Compressor) support.
@argument/String host
    @optional
@argument/Number port
    @optional
@argument/String dbname
@callback
*/
function getDatabase (host, port, dbname, callback) {
    if (arguments.length == 2) {
        dbname = host;
        callback = port;
        host = port = undefined;
    } else if (arguments.length == 3) {
        callback = dbname;
        dbname = port;
        port = undefined;
    }

    var dbsrv = new MongoDB.Server (host || '127.0.0.1', port || 27017);
    rawDatabase = new MongoDB.mongoloid (
        dbname,
        dbsrv,
        { w:0 }
    );
    rawDatabase.open (function (err) {
        if (err) return callback (err);
        rawDatabase.collection ('Mins', function (err, minsCollection) {
            if (err) return callback (err);
            minsCollection.ensureIndex ({ p:1, l:1 }, { unique:true }, function (err) {
                if (err) return callback (err);
                minsCollection.ensureIndex ({ p:1, s:1 }, function (err) {
                    if (err) return callback (err);
                    callback (undefined, new Database (dbname, new Database (rawDatabase)));
                });
            });
        });
    });
}


/**     @property/Function getRawDatabase
    Return a wrapped database [instance](waxwork.Database) instance with inline [compression]
    (waxwork.Compressor) support.
@argument/String host
    @optional
@argument/Number port
    @optional
@argument/String dbname
@callback
*/
function getRawDatabase (host, port, dbname, callback) {
    if (arguments.length == 2) {
        dbname = host;
        callback = port;
        host = port = undefined;
    } else if (arguments.length == 3) {
        callback = dbname;
        dbname = port;
        port = undefined;
    }

    var dbsrv = new MongoDB.Server (host || '127.0.0.1', port || 27017);
    rawDatabase = new MongoDB.mongoloid (
        dbname,
        dbsrv,
        { w:0 }
    );
    rawDatabase.open (function (err) {
        if (err) return callback (err);
        rawDatabase.collection ('Mins', function (err, collection) {
            if (err) return callback (err);
            collection.ensureIndex ({ p:1, l:1 }, { unique:true }, function (err) {
                if (err) return callback (err);
                collection.ensureIndex ({ p:1, s:1 }, function (err) {
                    if (err) return callback (err);
                    callback (undefined, rawDatabase);
                });
            });
        });
    });
}


/**     @property/Function collection
    Return a wrapped MongoDB [collection](mongodb.Collection) with inline [compression]
    (waxwork.Compressor) support.
@argument/String host
    @optional
@argument/Number port
    @optional
@argument/String dbname
@argument/String collectionName
@callback
*/
function collection (host, port, dbname, collectionName, callback) {
    if (arguments.length == 3) {
        callback = dbname;
        collectionName = port;
        dbname = host;
        host = port = undefined;
    } else if (arguments.length == 4) {
        callback = collectionName;
        collectionName = dbname;
        dbname = port;
        port = undefined;
    }

    var dbsrv = new MongoDB.Server (host || '127.0.0.1', port || 27017);
    var rawDatabase = new MongoDB.mongoloid (
        dbname,
        dbsrv,
        { w:0 }
    );
    rawDatabase.open (function (err) {
        if (err) return callback (err);
        var compressor = new Compressor (dbname, collectionName);
        compressor.ready (function (err) {
            if (err) return callback (err);
            rawDatabase.collection (collectionName, function (err, rawCollection) {
                if (err) return callback (err);
                callback (undefined, new Collection (collectionName, rawCollection, compressor));
            });
        });
    });
}


/**     @property/Function getRawCollection
    Return a raw [mongodb.Collection]() instance with no special effects.
@argument/String host
    @optional
@argument/Number port
    @optional
@argument/String dbname
@argument/String collectionName
@callback
    @argument/Error|undefined err
    @argument/.Collection|undefined collection
*/
function getRawCollection (host, port, dbname, collectionName, callback) {
    if (arguments.length == 3) {
        callback = dbname;
        collectionName = port;
        dbname = host;
        host = port = undefined;
    } else if (arguments.length == 4) {
        callback = collectionName;
        collectionName = dbname;
        dbname = port;
        port = undefined;
    }

    var dbsrv = new MongoDB.Server (host || '127.0.0.1', port || 27017);
    var rawDatabase = new MongoDB.Db (
        dbname,
        dbsrv,
        { w:0 }
    );
    rawDatabase.open (function (err) {
        if (err) return callback (err);
        rawDatabase.collection (collectionName, callback);
    });
}

module.exports.getDatabase      = getDatabase;
module.exports.getRawDatabase   = getRawDatabase;
module.exports.collection    = collection;
module.exports.getRawCollection = getRawCollection;
