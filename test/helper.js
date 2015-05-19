'use strict';

var chai    = require('chai');
var Knex    = require('knex');
var Promise = require('knex/lib/promise');

chai.config.includeStack = true;

chai.use(require('dirty-chai')); // avoid property assertions
chai.use(require('sinon-chai'));
require('mocha-sinon'); // provide `this.sinon` sandbox in tests

var expect  = module.exports.expect  = chai.expect;
var Promise = module.exports.Promise = Promise;

var database_config = {
  client: 'sqlite',
  connection: {
    filename: ':memory:'
  }
};

var knex = module.exports.knex = Knex(database_config);

before(function(done) {
  // migrate the DB once for each test suite
  knex.migrate
    .latest(database_config)
    .asCallback(done);
});

beforeEach(function(done) {
  var tables = [
    'jobs'
  ];

  Promise
    .map(tables, function(table) {
      return knex
        .del()
        .from(table);
    })
    .asCallback(function(err) {
      expect(err).to.not.exist();

      done();
    });
});
