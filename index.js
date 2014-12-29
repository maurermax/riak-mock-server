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
  this.sockets={};
  this.nextSocketId=1;
}

RiakMock.prototype.start = function(callback) {
  var self = this;
  this.server = this.app.listen(this.options.port, function() {
    callback(self.server.address().port);
  });
  this.server.on('connection', function (socket) {
    var socketId = self.nextSocketId++;
    self.sockets[socketId] = socket;
    socket.on('close', function () {
      delete self.sockets[socketId];
    });
  })
}

RiakMock.prototype.stop = function(callback) {
  this.server.close(callback);
  for (var socketId in this.sockets) {
    this.sockets[socketId].destroy();
  }
}

module.exports = RiakMock;