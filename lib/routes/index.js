var Database = require('../modules/database');
var express = require('express');
var bodyParser = require('body-parser');
var _ = require('lodash');

var Riak = function() {

  return {
    getClassName: function(obj) {
      if (obj && obj.constructor && obj.constructor.toString) {
        var arr = obj.constructor.toString().match(/function\s*(\w+)/);
        if (arr && arr.length == 2) {
          return arr[1];
        }
      }
      return undefined;
    },
    filterNotFound: function(values) {
      return values.filter(function(value, index, data) {
        if (typeof value === 'object') {
          return value['not_found'] === undefined;
        }
        else {
          return true;
        }
      });
    },
    mapValues: function(value, keyData, arg) {
      if (value["not_found"]) {
        return [value];
      }
      var data = value["values"][0]["data"];
      if (Riak.getClassName(data) !== "Array") {
        return [data];
      }
      else {
        return data;
      }
    },
    mapValuesJson: function(value, keyData, arg) {
      if (value["not_found"]) {
        return [value];
      }
      var newValues = Riak.mapValues(value, keyData, arg);
      return newValues.map(function(nv) {
        return JSON.parse(nv);
      });
    },
    mapByFields: function(value, keyData, fields) {
      if (!value.not_found) {
        var object = Riak.mapValuesJson(value)[0];
        for (field in fields) {
          if (object[field] != fields[field]) {
            return [];
          }
        }
        return [object];
      }
      else {
        return [];
      }
    },
    reduceSum: function(values, arg) {
      values = Riak.filterNotFound(values);
      if (values.length > 0) {
        return [values.reduce(function(prev, curr, index, array) {
          return prev + curr;
        })];
      }
      else {
        return [0];
      }
    },
    reduceMin: function(values, arg) {
      if (values.length == 0)
        return [];
      else
        return [values.reduce(function(prev, next) {
          return (prev < next) ? prev : next;
        })];
    },
    reduceMax: function(values, arg) {
      if (values.length == 0)
        return [];
      else
        return [values.reduce(function(prev, next) {
          return (prev > next) ? prev : next;
        })];
    },
    reduceSort: function(value, arg) {
      try {
        var c = eval(arg);
        return value.sort(c);
      }
      catch (e) {
        return value.sort();
      }
    },
    reduceNumericSort: function(value, arg) {
      value.sort(RiakHelper.numericSorter);
      return value;
    },
    reduceLimit: function(value, arg) {
      return value.slice(0, arg - 1);
    },
    reduceSlice: function(value, arg) {
      var start = arg[0];
      var end = arg[1];
      if (end > value.length) {
        return value;
      }
      else {
        return value.slice(start, end);
      }
    }
  };
}();

function Routes() {
  var database = new Database();
  var app = express();
  app.use(bodyParser.json());

  app.get('/buckets', function(req, res, next) {
    if (!req.query.buckets || req.query.buckets !== 'true') {
      return next(new Error('you need to call this with ?buckets=true'));
    }
    res.send({ buckets: database.getBuckets() });
  });

  function sendKeys(keySendType, res, keys) {
    if (keySendType && keySendType === 'true') {
      res.send({ keys: keys });
    } else {
      res.contentType('application/json');
      res.write(JSON.stringify({ keys: keys }));
      res.end();
    }
  }

  function isIntIndex(indexName) {
    return indexName.indexOf('_int', indexName.length - '_int'.length) !== -1;
  }

  app.get('/riak/:bucketName', function(req, res) {
    var bucket = database.getBucket(req.params.bucketName);
    return sendKeys(req.query.keys, res, bucket.getKeys());
  });

  var singleIndex = function(req, res) {
    var bucket = database.getBucket(req.params.bucketName);
    var indexName = req.params.indexName.toLowerCase();
    var indexValue = req.params.indexValue;
    if (isIntIndex(indexName)) {
      indexValue = parseInt(indexValue, 10);
    }
    var keys = bucket.getKeysForEqualityIndex(indexName, indexValue);
    return sendKeys(req.query.keys, res, keys);
  };
  app.get('/riak/:bucketName/index/:indexName/:indexValue', singleIndex);
  app.get('/buckets/:bucketName/index/:indexName/:indexValue', singleIndex);

  var rangeIndex = function(req, res) {
    var bucket = database.getBucket(req.params.bucketName);
    var keys = bucket.getKeysForRangeIndex(req.params.indexName, parseInt(req.params.indexFrom, 10), parseInt(req.params.indexTo, 10));
    return sendKeys(req.query.keys, res, keys);
  };
  app.get('/riak/:bucketName/index/:indexName/:indexFrom/:indexTo', rangeIndex);
  app.get('/buckets/:bucketName/index/:indexName/:indexFrom/:indexTo', rangeIndex);

  app.delete('/riak/:bucketName/:key', function(req, res, next) {
    var bucket = database.getBucket(req.params.bucketName);
    var key = req.params.key;
    if (!bucket.hasKey(key)) {
      return notFound(res);
    }
    res.status(204);
    bucket.deleteKey(key);
    res.end();
  });

  function notFound(res) {
    res.status(404);
    res.contentType("text/plain");
    res.send("not found");
    return res.end();
  }

  function setIndexHeaders(res, indexes) {
    _.forOwn(indexes, function(value, key) {
      res.header('x-riak-index-' + key, value);
    });
  }

  app.get('/riak/:bucketName/:key', function(req, res, next) {
    var bucket = database.getBucket(req.params.bucketName);
    var key = req.params.key;
    if (!bucket.hasKey(key)) {
      return notFound(res);
    }
    res.header("X-Riak-Vclock", bucket.getKey(key).vclock);
    setIndexHeaders(res, bucket.getIndexesForKey(key));
    res.contentType('application/json');
    res.write(JSON.stringify(bucket.getDataForKey(key)));
    res.end();
  });

  function getIndexesFromHeaders(headers) {
    var indexes = {};
    var regex = /^x-riak-index-(.*)$/;
    _.forOwn(headers, function(value, header) {
      var matches = header.match(regex);
      if (matches) {
        var key = matches[1];
        if (isIntIndex(key)) {
          value = parseInt(value, 10)
        }
        indexes[key] = value;
      }
    });
    return indexes;
  }

  app.put('/riak/:bucketName/:key', function(req, res, next) {
    var bucket = database.getBucket(req.params.bucketName);
    bucket.putDataForKey(req.params.key, req.body);
    bucket.putIndexesForKey(req.params.key, getIndexesFromHeaders(req.headers));
    res.status(204);
    res.end();
  });

  //Not complete, see http://docs.basho.com/riak/1.4.12/dev/references/keyfilters/
  var filterKeyHandler = {

    greater_than: function(key, val) {
      return val > key;
    },
    less_than: function(key, val) {
      return val < key
    },
    greater_than_eq: function(key, val) {
      return val >= key;
    },
    less_than_eq: function(key, val) {
      return val <= key
    },
    neq: function(key, val) {
      return key != val
    },
    eq: function(key, val) {
      return key == val
    },
    ends_with: function(key, val) {
      return key.indexOf(val, key.length - val.length) !== -1
    },
    starts_with: function(key, val) {
      return key.indexOf(val) === 0;
    }
  };

  function filterKeys(object, type, val) {
    var retObj = {};
    for (var key in object) {
      if (filterKeyHandler[type] && filterKeyHandler[type](key, val)) {
        retObj[key] = object[key];
      }
    }
    return retObj;
  }

  app.post('/mapred', function(req, res, next) {
    res.contentType('application/json');
    var bucketName;
    if (_.isObject(req.body.inputs)) {
      bucketName = req.body.inputs.bucket;
    } else if (_.isString(req.body.inputs)) {
      bucketName = req.body.inputs;
    }
    if (!bucketName) {
      res.send(new Error("bucket name not found"));
    }
    var bucket = database.getBucket(bucketName);
    var objects;
    if (req.body.inputs.index && req.body.inputs.key) {
      objects = bucket.getObjectsForEqualityIndex(req.body.inputs.index, req.body.inputs.key);
    } else if (req.body.inputs.index && req.body.inputs.start) {
      objects = bucket.getObjectsForRangeIndex(req.body.inputs.index, req.body.inputs.start, req.body.inputs.end);
    } else {
      objects = bucket.getObjects();
    }
    var args = req.body.query[0].map.arg;
    if (_.isArray(req.body.inputs.key_filters)) {
      var filters = req.body.inputs.key_filters;
      _.each(filters, function(filter) {
        var name = filter[0];
        var val = filter[1];
        objects = filterKeys(objects, name, val);
      });
    }
    eval("var mappingFunction = " + req.body.query[0].map.source);
    var result = [];
    _.forOwn(objects, function(object, key) {
      var mappingObject = {
        key: key,
        values: [{ metadata: { index: object.indexes }, indexes: object.indexes, data: JSON.stringify(object.data) }]
      };
      result.push.apply(result, mappingFunction(mappingObject, key, args));
    });
    res.write(JSON.stringify(result));
    res.end();
  });

  return app;
}

module.exports = Routes;