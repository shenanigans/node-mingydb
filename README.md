node-mingydb
============
Drop-in automagic compressing driver for [MongoDB](http://www.mongodb.org/). Allows you to query,
update, index, administrate, aggregate and otherwise perturb a MongoDB instance in an optimally
minified fashion without sacrificing legibility, sanity, or goats.

`mingydb` is early software. It has respectable test coverage but so far has not experienced
significant testing in the wild.


Installation
------------
```shell
$ npm install mingydb
```


Using MingyDB
-------------
`mingydb` is almost identical to the
[standard driver](http://mongodb.github.io/node-mongodb-native/1.4/) `mongodb`.

```javascript
// var mongodb = require ('mongodb');
var mongodb = require ('mingydb');

// connect with Db
var Db = new mongodb.Db ('databaseName', serverInstance);
Db.open (function (err) {
    Db.collection ("collectionName", perturbCollection);
});

// connect with MongoClient
mongodb.open (serverInstance, function (err, client) {
    client.collection (
        "collectionName",
        perturbCollection
    );
});

// connect with connect
mongodb.connect (
    "mongodb://mongo.example.com:9001/databaseName",
    options,
    function (err, client) {
        client.collection (
            "collectionName",
            perturbCollection
        );
    }
);

// try this easy new method
mongodb.collection (
    "databaseName",
    "collectionName",
    new mongodb.Server ("127.0.0.1", 27017),
    perturbCollection
);
```

A collection called `_mins` will be created in each database. If you plan to bombard your cluster
with new paths, you may shard this collection on the existing index `p_1_l_1`.

####API Differences
 * Callbacks are never optional.
 * Nothing officially deprecated is supported.
 * MapReduce is not supported. Use aggregation.
 * The `$where` operator will fail.
 * You may pass a GeoJSON point to `geoNear` instead of a legacy pair.
 * `geoHaystackSearch` passes a simple Array of records.

####Aggregation Notes
 * Cannot perform a recursive `$redact` on a compressed collection (the test key's minified form won't stay consistent as you `$$DESCEND`)
 * Aggregation stages must not transplant compressed subdocuments from one key to another or the entire document will become unrecoverable. Use `setDecompressed` or wait for `setAlias` to be ready.
 * Writing `$out` to another collection requires at least one `$group` or `$project` stage. During the **first** such stage, the path namespace shifts automatically from the first collection to the second.

####Geospatial Notes
To use a geospatial index with GeoJSON or legacy pairs with named fields, it is necessary to mark
part of the document as uncompressed. To do this, use `Collection#setUncompressed`.
```javascript
mingydb.collection (
    "databaseName",
    "collectionName",
    new mingydb.Server ('127.0.0.1', 27017),
    function (err, collection) {
        collection.setUncompressed ('location', function (err) {
            collection.ensureIndex ({ location:'2dsphere' }, function (err) {
                // ready for geospatial operations

            });
        });
    }
);
```


Development
-----------
`mingydb` is developed and maintained by Kevin "Schmidty" Smith under the MIT license. If you want to
see continued development, please help me [pay my bills!](https://www.paypal.com/cgi-bin/webscr?cmd=_donations&business=PN6C2AZTS2FP8&lc=US&currency_code=USD&bn=PP%2dDonationsBF%3abtn_donate_SM%2egif%3aNonHosted)


LICENSE
-------
The MIT License (MIT)

Copyright (c) 2014 Kevin "Schmidty" Smith

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

