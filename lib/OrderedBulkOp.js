
/**     @class mingydb.OrderedBulkOp
    @root
    A wrapped bulk operation command which executes its queued jobs in order.
@argument/mongodb.OrderedBulkOp mongoloid
*/
function OrderedBulkOp (mongoloid) {
    this.mongoloid = mongoloid;
}


/**     @member/Function insert

*/
OrderedBulkOp.prototype.insert = function (rec) {

};


/**     @member/Function find

*/
OrderedBulkOp.prototype.find = function (rec) {

};


/**     @class Context

*/
function Context (mongoloid) {
    this.mongoloid = mongoloid;
}


/**     @member/Function Context#remove
    Remove all selected documents from the database.
*/
Context.prototype.remove = function(){

};


/**     @member/Function Context#removeOne
    Remove the first document from the database.
*/
Context.prototype.removeOne = function(){

};


/**     @member/Function Context#replaceOne
    Replaces the first document with another document.
*/
Context.prototype.replaceOne = function (replacement) {

};


/**     @member/Function Context#update
    Updates every matching document.
*/
Context.prototype.update = function (change) {

};


/**     @member/Function Context#updateOne
    Updates the first matching document.
*/
Context.prototype.updateOne = function (change) {

};


/**     @member/Function Context#upsert
    Sets the `upsert` option to `true`.
*/
Context.prototype.upsert = function(){

};


module.exports = OrderedBulkOp;
