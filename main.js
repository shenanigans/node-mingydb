
/**     @module mingydb
    Automatic compression layer for MongoDB.
*/

var url         = require ('url');
var async       = require ('async');
var Database    = require ('./lib/Database');
var Collection  = require ('./lib/Collection');
var Compressor  = require ('./lib/Compressor');
var Client      = require ('./lib/Client');
var mongodb     = require ('mongodb');
var MongoClient = mongodb.MongoClient;
var Db          = mongodb.Db;

module.exports.Compressor = Compressor;

/**     @property/Function open
    Open a connection pool to the database and create a [Client](.Client) instance.
@argument/.Server server
@argument/JSON options
    @optional
@callback
    @argument/Error|undefined err
    @argument/.Client client
*/
function open (server, options, callback) {
    if (arguments.length == 2) {
        callback = options;
        options = { w:0 };
    } else
        if (!Object.hasOwnProperty.call (options, 'w'))
            options.w = 0;

    (new MongoClient (server, options)).open (function (err, mongoloid) {
        if (err) return callback (err);
        callback (undefined, new Client (mongoloid));
    });
};

/**     @property/Function rawOpen
    Open a connection pool to the database and create native a [MongoClient](mongodb.MongoClient)
    instance.
@argument/mongodb.Server server
@argument/JSON options
    @optional
@callback
    @argument/Error|undefined err
    @argument/.Client client
*/
function rawOpen (server, options, callback) {
    if (arguments.length == 2) {
        callback = options;
        options = { w:0 };
    } else
        if (!Object.hasOwnProperty.call (options, 'w'))
            options.w = 0;

    (new MongoClient (server, options)).open (callback);
};

/**     @property/Function connect
    Open a connection pool to the database and create a [Database](.Database) instance.
@argument/String dbURL
@argument/JSON options
    @optional
@callback
    @argument/Error|undefined err
    @argument/.Database db
*/
function connect (dbURL, options, callback) {
    if (arguments.length == 2) {
        callback = options;
        options = { w:0 };
    } else
        if (!Object.hasOwnProperty.call (options, 'w'))
            options.w = 0;

    MongoClient.connect (dbURL, options, function (err, mongoloid) {
        if (err) return callback (err);
        callback (undefined, new Database (url.parse (dbURL).hostname, mongoloid));
    });
};

/**     @property/Function rawConnect
    Open a connection pool to the database and create a native [Db](mongodb.Db) instance.
@argument/String dbURL
@argument/JSON options
    @optional
@callback
    @argument/Error|undefined err
    @argument/mongodb.Db db
*/
function rawConnect (dbURL, options, callback) {
    if (arguments.length == 2) {
        callback = options;
        options = { w:0 };
    } else
        if (!Object.hasOwnProperty.call (options, 'w'))
            options.w = 0;

    MongoClient.connect (dbURL, options, callback);
};

/**     @property/Function database
    Open a connection pool to the database and create a [Database](.Database) instance.
@argument/.Server server
@argument/JSON options
    @optional
@callback
    @argument/Error|undefined err
    @argument/.Database db
*/
function database (dbName, server, options, callback) {
    if (arguments.length == 3) {
        callback = options;
        options = { w:0 };
    } else
        if (!Object.hasOwnProperty.call (options, 'w'))
            options.w = 0;

    var mongoloid = new Db (dbName, server, options);
    mongoloid.open (function (err) {
        if (err) return callback (err);
        callback (undefined, new Database (dbName, mongoloid));
    });
}

/**     @property/Function rawDatabase
    Open a connection pool to the database and create a native [Db](mongodb.Db) instance.
@argument/mongodb.Server server
@argument/JSON options
    @optional
@callback
    @argument/Error|undefined err
    @argument/mongodb.Db db
*/
function rawDatabase (dbName, server, options, callback) {
    if (arguments.length == 3) {
        callback = options;
        options = { w:0 };
    } else
        if (!Object.hasOwnProperty.call (options, 'w'))
            options.w = 0;

    var mongoloid = new Db (dbName, server, options);
    mongoloid.open (function (err) {
        callback (err, mongoloid);
    });
}

/**     @property/Function collection
    Open a connection pool to the database and select a collection to create a [Collection]
    (.Collection) instance.
@argument/String databaseName
@argument/String collectionName
@argument/.Server server
@argument/JSON options
    @optional
@callback
    @argument/Error|undefined err
    @argument/.Collection collection
*/
function collection (dbName, colName, server, options, callback) {
    if (arguments.length == 4) {
        callback = options;
        options = { w:0 };
    } else if (!options)
        options = { w:0 };
    else if (!Object.hasOwnProperty.call (options, 'w'))
        options.w = 0;

    var mongoloid = new Db (dbName, server, options);
    mongoloid.open (function (err) {
        if (err) return callback (err);
        (new Database (dbName, mongoloid)).collection (colName, callback);
    });
}

/**     @property/Function rawCollection
    Open a connection pool to the database and select a collection to create a native [Collection]
    (mongodb.Collection) instance.
@argument/String databaseName
@argument/String collectionName
@argument/.Server server
@argument/JSON options
    @optional
@callback
    @argument/Error|undefined err
    @argument/mongodb.Collection collection
*/
function rawCollection (dbName, colName, server, options, callback) {
    if (arguments.length == 4) {
        callback = options;
        options = { w:0 };
    } else
        if (!Object.hasOwnProperty.call (options, 'w'))
            options.w = 0;

    var mongoloid = new Db (dbName, server, options);
    mongoloid.open (function (err) {
        if (err) return callback (err);
        mongoloid.collection (colName, callback);
    });
}

module.exports.open             = open;
module.exports.rawOpen          = rawOpen;
module.exports.connect          = connect;
module.exports.rawConnect       = rawConnect;
module.exports.database         = database;
module.exports.rawDatabase      = rawDatabase;
module.exports.collection       = collection;
module.exports.rawCollection    = rawCollection;

module.exports.Database         = Database;
module.exports.Db               = Database;
module.exports.Collection       = Collection;
module.exports.Server           = mongodb.Server;
