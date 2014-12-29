var Bucket = require('./bucket');

function Database() {
  this.buckets = {};
}

Database.prototype.getBucket = function(name) {
  if (!this.buckets.hasOwnProperty(name)) {
    this.buckets[name] = new Bucket(name);
  }
  return this.buckets[name];
};

Database.prototype.getBuckets = function() {
  return Object.keys(this.buckets);
};

module.exports = Database;