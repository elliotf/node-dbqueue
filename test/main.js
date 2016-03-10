'use strict';

var _       = require('lodash');
var async   = require('async');
var helper  = require('./helper.js');
var expect  = helper.expect;
var DBQueue = require('../');
var uuid    = require('uuid');
var db      = helper.test_db;

function userFacingJob(job_row) {
  return _.pick(job_row, 'category', 'data');
}

function withoutTimestamps(job_row) {
  return _.omit(job_row, 'create_time', 'update_time', 'id', 'locked_until');
}

describe('DBQueue', function() {
  var queue_options;
  var fake_uuid;

  beforeEach(function() {
    queue_options = {
    };

    fake_uuid = 'fakeuuid-0000-1111-2222-333333333333';
    this.sinon.stub(uuid, 'v4').returns(fake_uuid)
  });

  it('can be instantiated', function() {
    var queue = new DBQueue({});
  });

  describe('.connect', function() {
    it('returns a DBQueue instance', function(done) {
      DBQueue.connect(queue_options, function(err, queue) {
        expect(err).to.not.exist();

        expect(queue).to.be.an.instanceof(DBQueue);

        return done();
      });
    });
  });

  describe('#insert', function() {
    var queue;

    beforeEach(function(done) {
      DBQueue.connect(helper.test_db_config, function(err, result) {
        expect(err).to.not.exist();

        queue = result;

        return done();
      });
    });

    it('inserts a message onto the queue', function(done) {
      queue.insert('waffles', '{"example":"message data"}', function(err) {
        expect(err).to.not.exist();

        db.query('SELECT * FROM jobs', function(err, rows) {
          expect(err).to.not.exist();

          var actual = rows.map(withoutTimestamps);

          expect(actual).to.deep.equal([
            {
              data:          '{"example":"message data"}',
              queue:         'waffles',
              worker:        'unassigned',
            }
          ])

          return done();
        });
      });
    });
  });

  describe('#consume', function() {
    var queue;

    beforeEach(function(done) {
      DBQueue.connect(helper.test_db_config, function(err, result) {
        expect(err).to.not.exist();

        queue = result;

        return done();
      });
    });

    context('when there are jobs', function() {
      beforeEach(function(done) {
        var todo = [];

        todo.push(function(done) {
          queue.insert('queue_a', 'fake data for a', function(err) {
            expect(err).to.not.exist();

            return done();
          });
        });

        todo.push(function(done) {
          queue.insert('queue_b', 'fake data for b', function(err) {
            expect(err).to.not.exist();

            return done();
          });
        });

        async.parallel(todo, function(err) {
          expect(err).to.not.exist();

          return done();
        });
      });

      it('returns a job from the queue', function(done) {
        queue.consume('queue_a', function(err, job) {
          expect(err).to.not.exist();
          expect(withoutTimestamps(job)).to.deep.equal({
            queue:  'queue_a',
            data:   'fake data for a',
            worker: 'fakeuuid-0000-1111-2222-333333333333',
          });

          return done();
        });
      });

      it('gives a job out only once', function(done) {
        queue.consume('queue_a', function(err, job) {
          expect(err).to.not.exist();

          expect(job).to.exist();

          queue.consume('queue_a', function(err, job) {
            expect(err).to.not.exist();

            expect(job).to.be.undefined();

            return done();
          });
        });
      });

      it('leaves the job on the queue with an updated lock time', function(done) {
        queue.consume('queue_a', function(err, job) {
          expect(err).to.not.exist();

          db.query("SELECT * FROM jobs WHERE queue='queue_a'", [], function(err, rows) {
            expect(err).to.not.exist();

            expect(rows).to.have.length(1);
            expect(rows[0].locked_until).to.be.afterTime(new Date());

            return done();
          });
        });
      });

      context('and more than one queue is specified', function() {
        it('returns jobs from any of the specified queues', function(done) {
          queue.consume(['queue_a','queue_b'], function(err, job) {
            expect(err).to.not.exist();

            expect(job).to.exist();

            queue.consume(['queue_a','queue_b'], function(err, job) {
              expect(err).to.not.exist();

              expect(job).to.exist();

              queue.consume(['queue_a','queue_b'], function(err, job) {
                expect(err).to.not.exist();
                expect(job).to.not.exist();

                return done();
              });
            });
          });
        });
      });

      context('and the job completion callback is called', function() {
        it('removes the job from the queue', function(done) {
          queue.consume('queue_a', function(err, job, finished) {
            expect(err).to.not.exist();

            expect(finished).to.be.a('function');

            finished(function(err) {
              expect(err).to.not.exist();

              db.query("SELECT * FROM jobs WHERE queue='queue_a'", [], function(err, rows) {
                expect(err).to.not.exist();

                expect(rows).to.have.length(0);

                return done();
              });
            });
          });
        });

        context('more than once', function() {
          it('removes the job from the queue without error', function(done) {
            queue.consume('queue_a', function(err, job, finished) {
              expect(err).to.not.exist();

              expect(finished).to.be.a('function');

              finished();

              finished(function(err) {
                expect(err).to.not.exist();

                db.query("SELECT * FROM jobs WHERE queue='queue_a'", [], function(err, rows) {
                  expect(err).to.not.exist();

                  expect(rows).to.have.length(0);

                  return done();
                });
              });
            });
          });
        });

        context('without a callback', function() {
          it('removes the job from the queue without error', function(done) {
            queue.consume('queue_a', function(err, job, finished) {
              expect(err).to.not.exist();

              expect(finished).to.be.a('function');

              finished();

              setTimeout(function() {
                db.query("SELECT * FROM jobs WHERE queue='queue_a'", [], function(err, rows) {
                  expect(err).to.not.exist();

                  expect(rows).to.have.length(0);

                  return done();
                });
              }, 100);
            });
          });
        });
      });
    });

    context('when the desired queue is empty', function() {
      it('returns nothing', function(done) {
        queue.consume('queue_a', function(err, job) {
          expect(err).to.not.exist();

          expect(job).to.not.exist();

          return done();
        });
      });
    });

    context('when the desired queue does not exist', function() {
      it('returns nothing', function(done) {
        queue.consume('queue_c', function(err, job) {
          expect(err).to.not.exist();
          expect(job).to.not.exist();

          return done();
        });
      });
    });
  });
});
