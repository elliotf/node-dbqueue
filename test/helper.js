'use strict';

(function doNotRunInProduction() {
  var environment    = process.env.NODE_ENV || '';
  var okay_to_delete = {
    ci:        true,
    test:      true,
    localdev:  true,
    localtest: true
  };

  if (!okay_to_delete[environment]) {
    console.error("!!!\n!!! Tests should only be run in a test environment.  Aborting.\n!!!");
    process.exit(1);
  }
})();

var fs     = require('fs');
var chai   = require('chai');
var expect = exports.expect = chai.expect;
chai.use(require('dirty-chai'));
chai.use(require('sinon-chai'));
chai.use(require('chai-datetime'));
chai.config.includeStack = true;

require('mocha-sinon');

var mysql = require('mysql');

exports.test_db_config = {
  host:             '127.0.0.1',
  user:             'root',
  password:         '',
  database:         'dbqueue_testing_db',
  connectionLimit:  10,
};

var db = exports.test_db = mysql.createPool(exports.test_db_config);

before(function(done) {
  // init the DB schema
  db.query("SHOW TABLES LIKE 'jobs'", function(err, rows, fields) {
    expect(err).to.not.exist();

    if (rows.length) {
      return done();
    }

    fs.readFile(__dirname + '/../schema.sql', function(err, buffer) {
      expect(err).to.not.exist();

      var sql = buffer.toString();

      db.query(sql, function(err) {
        expect(err).to.not.exist();

        return done();
      });
    });
  });
});

beforeEach(function(done) {
  db.query('DELETE FROM jobs', function(err, rows, fields) {
    expect(err).to.not.exist();

    return done();
  });
});

/*
create table `attempt_batches` (
  `id` bigint unsigned not null auto_increment primary key,
  `payer_account_id` bigint not null,
  `proposal_id` bigint not null,
  `status` enum('pending', 'processing', 'submitted', 'failed') default 'pending',
  `create_time` timestamp default CURRENT_TIMESTAMP,
  `update_time` timestamp default '1970-01-02 00:00:00'
);
*/
