var _ = require('lodash');

function Bucket(name) {
  this.name = name;
  this.data = {};
}

Bucket.prototype.getKeys = function() {
  return Object.keys(this.data);
}

Bucket.prototype.getIndexesForKey = function(key) {
  return this.data[key].indexes;
}

Bucket.prototype.getDataForKey = function(key) {
  return this.data[key].data;
}

Bucket.prototype.getKey = function(key) {
  return this.data[key];
}


Bucket.prototype.deleteKey = function(key) {
  delete this.data[key];
}

function getEmptyKey(key) {
  return {
    data: {},
    indexes: {},
    vclock: 0
  }
}

Bucket.prototype.putDataForKey = function(key, data) {
  if (!this.hasKey(key)) {
    this.data[key] = getEmptyKey(key);
  }
  this.data[key].data = data;
  this.data[key].vclock++;
}

Bucket.prototype.putIndexesForKey = function(key, indexes) {
  if (!this.hasKey(key)) {
    this.data[key] = getEmptyKey(key);
  }
  this.data[key].indexes = indexes;
}

function objectFilter(object, evaluator) {
  var retObj = {};
  for (var key in object) {
    if (object.hasOwnProperty(key)) {
      var val = object[key];
      if (evaluator(val)) {
        retObj[key] = val;
      }
    }
  }
  return retObj;
}

Bucket.prototype.getObjectsForEqualityIndex = function(indexName, indexValue) {
  indexName = indexName.toLowerCase();
  return objectFilter(this.data, function(val) {
    return val.indexes && val.indexes[indexName] === indexValue;
  });
}

Bucket.prototype.getObjects = function() {
  return this.data;
}

Bucket.prototype.getObjectsForRangeIndex = function(indexName, indexStart, indexEnd) {
  indexName = indexName.toLowerCase();
  return objectFilter(this.data, function(val) {
    return val.indexes && val.indexes[indexName] >= indexStart && val.indexes[indexName] <= indexEnd
  });
}

Bucket.prototype.getKeysForEqualityIndex = function(indexName, indexValue) {
  return Object.keys(this.getObjectsForEqualityIndex(indexName, indexValue));
}

Bucket.prototype.getKeysForRangeIndex = function(indexName, indexStart, indexEnd) {
  return Object.keys(this.getObjectsForRangeIndex(indexName, indexStart, indexEnd));
}

Bucket.prototype.hasKey = function(key) {
  return this.data.hasOwnProperty(key);
}


module.exports = Bucket;