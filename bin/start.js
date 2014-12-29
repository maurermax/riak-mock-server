var RiakMock = require('../index.js');
var riakMock = new RiakMock();
riakMock.start(function(port) {
  console.log('started server on port %j', port);
})
