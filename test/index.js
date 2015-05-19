var helper = require('./helper');
var Queue  = require('../');
var db     = helper.knex;
var expect = helper.expect;

describe('Queue', function() {
  it('can be instantiated', function() {
    var queue = new Queue(db);
  });
});
