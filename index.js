var express = require('express');
var Routes = require('./lib/routes');
var _ = require('lodash');

function RiakMock(options) {
  var defaultOptions = {
    port: 0
  }
  this.app = express();
  this.app.use(new Routes());
  this.options = _.defaults(options, defaultOptions);
}

RiakMock.prototype.start = function(callback) {
  var self = this;
  this.server = this.app.listen(this.options.port, function() {
    callback(self.server.address().port);
  });
}

RiakMock.prototype.stop = function(callback) {
  this.server.close(callback);
}

module.exports = RiakMock;