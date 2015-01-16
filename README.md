node-mingydb
============
Drop-in automagic compressing driver for [MongoDB](http://www.mongodb.org/). Allows you to query,
update, index, administrate, aggregate and otherwise perturb a MongoDB instance in an optimally
minified fashion without sacrificing legibility, sanity, or goats.

`mingydb` is early software. It has respectable test coverage but has yet to undergo significant
testing in the wild.


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
```

The api differs in a few ways:
 * The `save` method is not supported.
 * Nothing officially deprecated is supported.
 * The `$where` operator will fail.
 * MapReduce is not supported. Use aggregation.
 * `Collection#find` and `Collection#findOne` have no `skip`, `limit`, or `timeout` arguments. When three arguments are provided, the second is **always** assumed to be a projection, **never** an `options` Object.
 * You may pass a GeoJSON point to `geoNear` instead of a legacy pair.
 * `geoHaystackSearch` returns a simple Array of records.

A collection called `_mins` will be created in each database. If you plan to bombard your cluster
with new paths, you may shard this collection on the existing index `p_1_l_1`.

To use a geospatial index with GeoJSON or legacy pairs with named fields, it is necessary to mark
part of the document as uncompressed. To do this, use `Collection#setUncompressed`.
```javascript
mingydb.collection (
    myDB,
    myCollection,
    aServer,
    function (err, collection) {
        collection.setUncompressed ('location', function (err) {
            // done

        });
    }
);
```


Documentation
-------------
Full documentation available [here.](https://shenanigans.github.io/node-mingydb/index.html)

For most purposes, you may refer to the documentation for the
[standard driver](http://mongodb.github.io/node-mongodb-native/1.4/).


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

