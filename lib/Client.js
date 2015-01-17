
/**     @class mingydb.Client
    @super events.EventEmitter
    @root
    A wrapped [MongoClient](mongodb.MongoClient).
*/
var Database = require ('./Database');

function getBoundDelegateMethod (owner, method) {
    return function(){ method.apply (owner, arguments); };
}
var emitterMethodNames = [
    'addListener',          'on',                   'once',                 'removeListener',
    'removeAllListeners',   'setMaxListeners',      'listeners',            'emit'
];
function Client (mongoloid) {
    this.mongoloid = mongoloid;
    // inherit event methods
    for (var i=0,j=emitterMethodNames.length; i<j; i++) {
        var name = emitterMethodNames[i];
        this[name] = getBoundDelegateMethod (mongoloid, mongoloid[name]);
    }
}

/**     @member/Function db
    Instantiate a [Database instance](mingydb.Database) on the connected server.
@returns/.Database
*/
Client.prototype.db = function (name) {
    return new Database (name, this.mongoloid.db (name));
};

/**     @member/Function close
    Disconnect from the connected server.
@callback
    @argument/Error|undefined err
*/
Client.prototype.close = function (callback) {
    this.mongoloid.close (callback);
};

module.exports = Client;
