node-mingydb
============
Drop-in automagic compressing driver for [MongoDB](http://www.mongodb.org/). Allows you to query,
update, index, administrate, aggregate and otherwise perturb a MongoDB instance in an optimally
minified fashion without sacrificing legibility, sanity, or goats.


Installation
------------
```shell
$ npm install mingydb
```


Using MingyDB
-------------
`mingydb` is almost identical to the [standard driver] `mongodb`. It is highly likely that switching
to `mingydb` will be about this easy:
```javascript
// var mongodb = require ('mongodb');
var mongodb = require ('mingydb');
```

For more information, consult the
[standard driver docs](http://mongodb.github.io/node-mongodb-native/1.4/).


Development
-----------
`mingydb` is developed and maintained by Kevin "Schmidty" Smith under the MIT license. If you want to
see continued development, please help me [pay my bills!](https://www.paypal.com/cgi-bin/webscr?cmd=_donations&business=PN6C2AZTS2FP8&lc=US&currency_code=USD&bn=PP%2dDonationsBF%3abtn_donate_SM%2egif%3aNonHosted)
