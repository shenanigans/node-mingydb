
/**     @class mingydb.CompressionError
    @super Error

@String #code
@String #message
*/

var util = require ('util');

function CompressionError (code, message, info) {
    Error.call (this, message);
    this.code = code;
    if (info) for (var key in info) this[key] = info[key];
}
util.inherits (CompressionError, Error);

module.exports = CompressionError;
