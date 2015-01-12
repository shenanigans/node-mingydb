
/**     @module mingydb

@spare details
    Wraps MongoDB connectivity and obfuscates document minification.
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

*/
function rawConnect (url, options, callback) {
    if (arguments.length == 2) {
        callback = options;
        options = { w:0 };
    } else
        if (!Object.hasOwnProperty.call (options, 'w'))
            options.w = 0;

    MongoClient.connect (url, options, callback);
};

/**     @property/Function getDatabase

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

/**     @property/Function getRawDatabase

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

/**     @property/Function getRawCollection

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
module.exports.Collection       = Collection;
module.exports.Server           = mongodb.Server;
