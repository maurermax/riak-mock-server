riak-mock-server
================

A riak mock server used to emulate a real riak server for unit testing purposes.

## How to use
You can either do
````
node ./bin/start.js
````
to get a running instance. Or use this module somewhere else to start the server by doing.
````
var RiakMockServer = require('riak-mock-server');
var riakMockServer = new RiakMockServer();
riakMockServer.start(function(port) {
 console.log('riak mock server has been started at port %j', port);
});
````

## Disclaimer
This module has been created as part of the testing development in one of my projects. It is *not* feature complete. Instead it only covers what we needed for our tests so far. Please feel free to fork and add to this.
