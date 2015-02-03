var request = require('request');
var chai = require('chai');
var expect = chai.expect;
var App = require('../lib/routes');
var _ = require('lodash');

describe('All tests', function() {
  var baseUrl;
  var server;
  beforeEach(function(done) {
    server = require('http').createServer(new App());
    server.listen(0, function() {
      baseUrl = 'http://localhost:' + server.address().port;
      done();
    });
  });

  afterEach(function(done) {
    server.close(done);
  });

  function populateBuckets(cb) {
    request.put({ url: baseUrl + '/riak/bucket1/key1', json: { content1: 'val1' }, headers: { "x-riak-index-text_bin": "indexVal1", "x-riak-index-number_int": "1" } }, function() {
      request.put({ url: baseUrl + '/riak/bucket2/key2', json: { content2: 'val2' }, headers: { "x-riak-index-text_bin": "indexVal2", "x-riak-index-number_int": "2" }}, cb);
    });
  }

  describe('GET /buckets?buckets=true', function() {
    it('returns an empty list of buckets', function(done) {
      var route = baseUrl + '/buckets?buckets=true';
      request(route, function(err, res) {
        expect(res.headers['content-type']).to.contain('json');
        expect(res.statusCode).to.equal(200);
        var content = JSON.parse(res.body);
        expect(content).to.have.property('buckets').with.length(0);
        done();
      });
    });

    it('returns an list of buckets list of buckets', function(done) {
      var route = baseUrl + '/buckets?buckets=true';
      populateBuckets(function() {
        request(route, function(err, res) {
          expect(res.headers['content-type']).to.contain('json');
          expect(res.statusCode).to.equal(200);
          var content = JSON.parse(res.body);
          expect(content).to.have.property('buckets').with.length(2);
          done();
        });
      });
    });
  });

  describe('GET /riak/:bucket?keys=true', function() {
    it('returns an empty list of keys if no keys in bucket', function(done) {
      var route = baseUrl + '/riak/bucket1?keys=true';
      request(route, function(err, res) {
        expect(res.headers['content-type']).to.contain('json');
        expect(res.statusCode).to.equal(200);
        var content = JSON.parse(res.body);
        expect(content).to.have.property('keys').with.length(0);
        done();
      });
    });

    it('returns a list of all keys in this bucket', function(done) {
      var route = baseUrl + '/riak/bucket1?keys=true';
      populateBuckets(function() {
        request(route, function(err, res) {
          expect(res.headers['content-type']).to.contain('json');
          expect(res.statusCode).to.equal(200);
          var content = JSON.parse(res.body);
          expect(content).to.have.property('keys').with.length(1);
          done();
        });
      });
    });

    it('with stream=true returns a list of all keys in this bucket in a chunked encoding', function(done) {
      var route = baseUrl + '/riak/bucket1?keys=stream';
      populateBuckets(function() {
        request(route, function(err, res) {
          expect(res.headers['transfer-encoding']).to.equal('chunked');
          expect(res.headers['content-type']).to.contain('json');
          expect(res.statusCode).to.equal(200);
          var content = JSON.parse(res.body);
          expect(content).to.have.property('keys').with.length(1);
          done();
        });
      });
    });
  });

  var prefixes = ['riak', 'buckets'];
  _.each(prefixes, function(prefix) {
    describe('GET /'+prefix+'/:bucket/index/:indexName/:indexValue', function() {
      it('returns an empty list of keys if no keys match this indexValue', function(done) {
        var route = baseUrl + '/'+prefix+'/bucket1/index/text_bin/notFoundVal';
        populateBuckets(function() {
          request(route, function(err, res) {
            expect(res.headers['content-type']).to.contain('json');
            expect(res.statusCode).to.equal(200);
            var content = JSON.parse(res.body);
            expect(content).to.have.property('keys').with.length(0);
            done();
          });
        });
      });

      it('returns a of matching keys if the index matches', function(done) {
        var route = baseUrl + '/'+prefix+'/bucket1/index/text_bin/indexVal1';
        populateBuckets(function() {
          request(route, function(err, res) {
            expect(res.headers['content-type']).to.contain('json');
            expect(res.statusCode).to.equal(200);
            var content = JSON.parse(res.body);
            expect(content).to.have.property('keys').with.length(1);
            done();
          });
        });
      });

      it('can also handle numeric indexes', function(done) {
        var route = baseUrl + '/'+prefix+'/bucket1/index/number_int/1';
        populateBuckets(function() {
          request(route, function(err, res) {
            expect(res.headers['content-type']).to.contain('json');
            expect(res.statusCode).to.equal(200);
            var content = JSON.parse(res.body);
            expect(content).to.have.property('keys').with.length(1);
            done();
          });
        });
      });
    });

    describe('GET /'+prefix+'/:bucket/index/:indexName/:indexFrom/:indexTo', function() {
      it('returns an empty list of keys if no keys match this indexValue', function(done) {
        var route = baseUrl + '/'+prefix+'/bucket1/index/number_int/1000/1001';
        populateBuckets(function() {
          request(route, function(err, res) {
            expect(res.headers['content-type']).to.contain('json');
            expect(res.statusCode).to.equal(200);
            var content = JSON.parse(res.body);
            expect(content).to.have.property('keys').with.length(0);
            done();
          });
        });
      });

      it('returns a of matching keys if the index range matches', function(done) {
        var route = baseUrl + '/'+prefix+'/bucket1/index/number_int/0/2';
        populateBuckets(function() {
          request(route, function(err, res) {
            expect(res.headers['content-type']).to.contain('json');
            expect(res.statusCode).to.equal(200);
            var content = JSON.parse(res.body);
            expect(content).to.have.property('keys').with.length(1);
            done();
          });
        });
      });
    });
  });

  describe('GET /riak/:bucket/:key', function() {
    it('returns a 404 with "not found" if the key does not exist', function(done) {
      var route = baseUrl + '/riak/bucket1/key1';
      request(route, function(err, res) {
        expect(res.statusCode).to.equal(404);
        expect(res.body).to.equal('not found');
        done();
      });
    });

    it('returns data for a get request', function(done) {
      var route = baseUrl + '/riak/bucket1/key1';
      populateBuckets(function() {
        request(route, function(err, res) {
          expect(res.headers['content-type']).to.contain('json');
          expect(res.statusCode).to.equal(200);
          var content = JSON.parse(res.body);
          expect(content).to.have.property('content1', 'val1');
          done();
        });
      });
    });
  });

  describe('PUT /riak/:bucket/:key', function() {
    it('inserts a piece of data successfully', function(done) {
      request.put({ url: baseUrl + '/riak/bucket1/key1', json: { content1: 'val1' } }, function(err, res) {
        expect(res.statusCode).to.equal(204);
        // TODO check that data has really been written
        done();
      });
    });
  });

  describe('DELETE /riak/:bucket/:key', function() {
    it('returns a 404 with "not found" if the key does not exist', function(done) {
      request.del(baseUrl + '/riak/bucket1/key1', function(err, res) {
        expect(res.statusCode).to.equal(404);
        expect(res.body).to.equal('not found');
        done();
      });
    });

    it('deletes a piece of data successfully', function(done) {
      populateBuckets(function() {
        request.del(baseUrl + '/riak/bucket1/key1', function(err, res) {
          expect(res.statusCode).to.equal(204);
          // TODO check that data has really been written
          done();
        });
      });
    });
  });

  describe('POST /mapred', function() {
    it('executes a proper mapreduce query with a single value query', function(done) {
      populateBuckets(function() {
        request.post({ url: baseUrl + '/mapred', json: {"inputs":{"bucket":"bucket1","index":"number_int","key":1},"query":[{"map":{"language":"javascript","source":"function(v) { return [{key: v.key, data: v.values[0].data}]; }","keep":true}}]} }, function(err, res) {
          expect(res.headers['content-type']).to.contain('json');
          expect(res.statusCode).to.equal(200);
          expect(res.body).to.have.length(1);
          expect(res.body[0]).to.have.property('key').that.equals('key1');
          done();
        });
      });
    });
    it('executes a proper mapreduce query with a range query', function(done) {
      populateBuckets(function() {
        request.post({ url: baseUrl + '/mapred', json: {"inputs":{"bucket":"bucket1","index":"number_int","start":0, "end":1},"query":[{"map":{"language":"javascript","source":"function(v) { return [{key: v.key, data: v.values[0].data}]; }","keep":true}}]} }, function(err, res) {
          expect(res.headers['content-type']).to.contain('json');
          expect(res.statusCode).to.equal(200);
          expect(res.body).to.have.length(1);
          expect(res.body[0]).to.have.property('key').that.equals('key1');
          done();
        });
      });
    });

    it('executes a proper mapreduce query with a ends_with key filter', function(done) {
      populateBuckets(function() {
        request.put({ url: baseUrl + '/riak/bucket1/key_endsWithA', json: { content1: 'val3' }, headers: { "x-riak-index-text_bin": "indexVal3", "x-riak-index-number_int": "3" } }, function() {
          request.put({ url: baseUrl + '/riak/bucket1/key_also_endsWithA', json: { content2: 'val4' }, headers: { "x-riak-index-text_bin": "indexVal4", "x-riak-index-number_int": "4" }}, function(){
            request.put({ url: baseUrl + '/riak/bucket1/key_endsWithA_Not', json: { content2: 'val5' }, headers: { "x-riak-index-text_bin": "indexVal5", "x-riak-index-number_int": "5" }}, function(){
              request.post({ url: baseUrl + '/mapred', json: {"inputs":{"bucket":"bucket1","index":"number_int","key_filters":[["ends_with", "endsWithA"]]},"query":[{"map":{"language":"javascript","source":"function(v) { return [{key: v.key, data: v.values[0].data}]; }","keep":true}}]} }, function(err, res) {
                expect(res.headers['content-type']).to.contain('json');
                expect(res.statusCode).to.equal(200);
                expect(res.body).to.have.length(2);
                done();
              });
            });
          });
        });

      });
    });

    it('executes a proper mapreduce query with a starts_with key filter', function(done) {
      populateBuckets(function() {
        request.put({ url: baseUrl + '/riak/bucket1/startsWithA_key', json: { content1: 'val3' }, headers: { "x-riak-index-text_bin": "indexVal3", "x-riak-index-number_int": "3" } }, function() {
          request.put({ url: baseUrl + '/riak/bucket1/startsWithA_key_also', json: { content2: 'val4' }, headers: { "x-riak-index-text_bin": "indexVal4", "x-riak-index-number_int": "4" }}, function(){
            request.put({ url: baseUrl + '/riak/bucket1/starts_not_WithA_key', json: { content2: 'val5' }, headers: { "x-riak-index-text_bin": "indexVal5", "x-riak-index-number_int": "5" }}, function(){
              request.post({ url: baseUrl + '/mapred', json: {"inputs":{"bucket":"bucket1","index":"number_int","key_filters":[["starts_with", "startsWithA"]]},"query":[{"map":{"language":"javascript","source":"function(v) { return [{key: v.key, data: v.values[0].data}]; }","keep":true}}]} }, function(err, res) {
                expect(res.headers['content-type']).to.contain('json');
                expect(res.statusCode).to.equal(200);
                expect(res.body).to.have.length(2);
                done();
              });
            });
          });
        });

      });
    });
    it('passes arguments to handling function', function(done) {
      populateBuckets(function() {
        request.post({ url: baseUrl + '/mapred', json: {"inputs":"bucket1","query":[{"map":{"language":"javascript","source":"function(v, key, args) { return [args.val]; }", "keep":true, "arg":{"val":"test"}}}]}}, function(err, res) {
          expect(res.statusCode).to.equal(200);
          expect(res.headers['content-type']).to.contain('json');
          expect(res.body).to.have.length(1);
          expect(res.body[0]).to.equal('test');
          done();
        });
      });
    });
  });
});
