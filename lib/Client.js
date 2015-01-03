
/**     @module mingydb     */
var Database = require ('./Database');

/**     @class Client
    @super EventEmitter

*/
function Client (mongoloid) {
    this.mongoloid = mongoloid;
}

/**     @member/Function Client#db

@returns/.Database
*/
Client.prototype.db = function (name) {
    return new Database (name, this.mongoloid.db (name));
};

/**     @member/Function Client#close

@callback
    @argument/Error|undefined err
*/
Client.prototype.close = function (callback) {
    this.mongoloid.close (callback);
};

module.exports = Client;
