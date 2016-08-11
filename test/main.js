'use strict';

var _       = require('lodash');
var async   = require('async');
var yaml    = require('js-yaml');
var uuid    = require('uuid');
var helper  = require('./helper.js');
var expect  = helper.expect;
var DBQueue = require('../');
var db      = helper.test_db;

function withoutTimestamps(job_row) {
  return _.omit(job_row, 'create_time', 'update_time', 'id', 'locked_until');
}

describe('DBQueue', function() {
  it('can be instantiated', function() {
    var queue = new DBQueue({});
  });

  describe('.connect', function() {
    it('returns a DBQueue instance', function(done) {
      DBQueue.connect(helper.test_db_config, function(err, queue) {
        expect(err).to.not.exist();

        expect(queue).to.be.an.instanceof(DBQueue);

        return done();
      });
    });

    context('when the database port is invalid', function() {
      it('yields an error rather than throw an exception', function(done) {
        var config = _.extend({}, helper.test_db_config, {
          port: 10,
        });

        DBQueue.connect(config, function(err, queue) {
          expect(err).to.exist();

          expect(err.code).to.equal('ECONNREFUSED');

          return done();
        });
      });
    });

    context('when the database hostname is invalid', function() {
      it('yields an error rather than throw an exception', function(done) {
        var config = _.extend({}, helper.test_db_config, {
          host: 'fake-domain-here.example',
        });

        DBQueue.connect(config, function(err, queue) {
          expect(err).to.exist();

          expect(err.code).to.equal('ENOTFOUND');

          return done();
        });
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
      queue.insert('waffles', {example:'message data'}, function(err) {
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
          ]);

          return done();
        });
      });
    });

    context('when called on a newly instantiated object', function() {
      var queue;

      beforeEach(function() {
        queue = new DBQueue(helper.test_db_config);
      });

      it('lazily connects to the datastore', function(done) {
        queue.insert('waffles', '{"example":"message data"}', function(err) {
          expect(err).to.not.exist();

          db.query('SELECT * FROM jobs', function(err, rows) {
            expect(err).to.not.exist();

            expect(rows).to.have.length(1);

            return done();
          });
        });
      });
    });

    context('when the database port is invalid', function() {
      it('yields an error rather than throw an exception', function(done) {
        var config = _.extend({}, helper.test_db_config, {
          port: 10,
        });

        queue = new DBQueue(config);

        queue.insert('queue name', 'fake message', function(err) {
          expect(err).to.exist();

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
        var fake_uuid = 'fakeuuid-0000-1111-2222-333333333333';
        this.sinon.stub(uuid, 'v4').returns(fake_uuid)

        queue.consume('queue_a', function(err, job) {
          expect(err).to.not.exist();
          expect(job).to.deep.equal('fake data for a');

          return done();
        });
      });

      it('gives a job out no more than once', function(done) {
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

            finished(null);

            setTimeout(function() {
              db.query("SELECT * FROM jobs WHERE queue='queue_a'", [], function(err, rows) {
                expect(err).to.not.exist();

                expect(rows).to.have.length(0);

                return done();
              });
            }, 10);
          });
        });

        context('more than once', function() {
          it('removes the job from the queue without error', function(done) {
            queue.consume('queue_a', function(err, job, finished) {
              expect(err).to.not.exist();

              expect(finished).to.be.a('function');

              finished();

              finished();

              setTimeout(function(err) {
                expect(err).to.not.exist();

                db.query("SELECT * FROM jobs WHERE queue='queue_a'", [], function(err, rows) {
                  expect(err).to.not.exist();

                  expect(rows).to.have.length(0);

                  return done();
                });
              }, 10);
            });
          });
        });

        context('with an error', function() {
          it('leaves the job on the queue', function(done) {
            queue.consume('queue_a', function(err, job, finishedWithJob) {
              expect(err).to.not.exist();

              finishedWithJob(new Error('fake error'));
              setTimeout(function(err) {
                expect(err).to.not.exist();

                db.query('SELECT * FROM jobs WHERE queue = ?', ['queue_a'], function(err, rows) {
                  expect(err).to.not.exist();

                  expect(rows).to.have.length(1);

                  return done();
                });
              },10);
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
              }, 10);
            });
          });
        });
      });
    });

    context('when there is more than one job', function() {
      beforeEach(function(done) {
        queue.insert('queue_a', 'first', function(err) {
          expect(err).to.not.exist();

          queue.insert('queue_a', 'second', function(err) {
            expect(err).to.not.exist();

            return done();
          });
        });
      });

      it('returns all of them one at a time', function(done) {
        queue.consume('queue_a', function(err, first) {
          expect(err).to.not.exist();

          expect(first).to.exist();

          queue.consume('queue_a', function(err, second) {
            expect(err).to.not.exist();

            expect(second).to.exist();

            expect(second).to.not.deep.equal(first);

            queue.consume('queue_a', function(err, last) {
              expect(err).to.not.exist();

              expect(last).to.not.exist();

              return done();
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

    context('when all of the jobs are locked', function() {
      it('returns nothing', function(done) {
        queue.insert('queue_a', 'some data', function(err) {
          expect(err).to.not.exist();

          queue.consume('queue_a', function(err, job) {
            expect(err).to.not.exist();

            queue.consume('queue_a', function(err, job) {
              expect(err).to.not.exist();

              expect(job).to.not.exist();

              return done();
            });
          });
        });
      });
    });

    context('when provided an options object', function() {
      var options;

      beforeEach(function(done) {
        options = {
        };

        queue.insert('a queue', 'message 1', function(err) {
          expect(err).to.not.exist();

          return done();
        });
      });

      context('that has a lock time', function() {
        beforeEach(function() {
          var hour_in_seconds = 60 * 60;
          options.lock_time = hour_in_seconds;
        });

        it('locks the jobs for that long', function(done) {
          queue.consume('a queue', options, function(err, job) {
            expect(err).to.not.exist();

            db.query("SELECT locked_until FROM jobs", function(err, rows) {
              expect(err).to.not.exist();

              expect(rows).to.have.length(1);

              var minute   = 60*1000;
              var expected = new Date(Date.now() + (45 * minute));
              expect(rows[0].locked_until).to.be.afterTime(expected);

              return done();
            });
          });
        });
      });

      context('that has a count', function() {
        beforeEach(function(done) {
          options.count = 2;

          queue.insert('a queue', 'message 2', function(err) {
            expect(err).to.not.exist();

            queue.insert('a queue', 'message 3', function(err) {
              expect(err).to.not.exist();

              return done();
            });
          });
        });

        it('calls the job handler that many times', function(done) {
          var consumer = this.sinon.spy();

          queue.consume('a queue', options, consumer);

          setTimeout(function() {
            expect(consumer).to.have.been.calledTwice();

            // called without err
            expect(consumer.args[0][0]).to.not.exist();
            expect(consumer.args[1][0]).to.not.exist();

            db.query("SELECT locked_until FROM jobs WHERE locked_until > NOW()", function(err, rows) {
              expect(err).to.not.exist();

              expect(rows).to.have.length(2);

              return done();
            });
          }, 10);
        });

        context('that is greater than the number of messages', function() {
          it('returns all available messages', function(done) {
            options.count = 100;
            var consumer = this.sinon.spy();

            queue.consume('a queue', options, consumer);

            setTimeout(function() {
              expect(consumer).to.have.been.calledThrice();

              // called without err
              expect(consumer.args[0][0]).to.not.exist();
              expect(consumer.args[1][0]).to.not.exist();
              expect(consumer.args[2][0]).to.not.exist();

              return done();
            }, 10);
          });
        });
      });
    });
  });

  describe('#listen', function() {
    var queue;
    var listen_options;

    beforeEach(function(done) {
      queue = new DBQueue(helper.test_db_config);

      listen_options  = {
        interval:        1000,
        lock_time:       5,
      };

      var todo = ['a', 'b', 'c', 'd', 'e'];
      async.map(
        todo,
        function(message, done) {
          return queue.insert('a queue', message, done);
        },
        function(err) {
          expect(err).to.not.exist();

          return done();
        }
      );
    });

    it('consumes messages on an interval', function(done) {
      this.sinon.stub(queue, 'consume');

      var clock    = this.sinon.useFakeTimers();
      var consumer = this.sinon.spy();

      queue.listen('a queue', listen_options, consumer);
      expect(queue.consume).not.to.have.been.called();
      clock.tick(5);
      expect(queue.consume).not.to.have.been.called();
      clock.tick(1000);

      expect(queue.consume).to.have.been.calledOnce();
      clock.tick(500);
      expect(queue.consume).to.have.been.calledOnce();
      clock.tick(1000);
      expect(queue.consume).to.have.been.calledTwice();

      return done();
    });

    it('can stop listening', function(done) {
      this.sinon.stub(queue, 'consume');

      var clock    = this.sinon.useFakeTimers();
      var consumer = this.sinon.spy();

      var stop = queue.listen('a queue', listen_options, consumer);
      clock.tick(1500);
      expect(queue.consume).to.have.been.calledOnce();
      clock.tick(1000);
      expect(queue.consume).to.have.been.calledTwice();
      stop();
      clock.tick(2000);
      expect(queue.consume).to.have.been.calledTwice();

      return done();
    });

    it('passes arguments through to DBQueue#consume', function(done) {
      this.sinon.stub(queue, 'consume');

      var clock    = this.sinon.useFakeTimers();
      var consumer = this.sinon.spy();

      var stop = queue.listen('a queue', listen_options, consumer);
      clock.tick(1500);
      expect(queue.consume).to.have.been.calledOnce();

      var expected_options = {
        lock_time: 5,
        count:     1,
      };

      expect(queue.consume).to.have.been.calledWith('a queue', expected_options);

      return done();
    });

    it('the number of messages being processed never exceeds `max_outstanding`', function(done) {
      var clock         = this.sinon.useFakeTimers();

      var num_processed = 0;
      function consumer(err, message, ackMessage) {
        setTimeout(ackMessage, 2000 * ++num_processed);
      }

      var fakeAckMessage = this.sinon.spy();
      var num_messages   = 0;
      var max_messages   = 5;
      this.sinon.stub(queue, 'consume', function(queue_name, consume_options, done) {
        if (num_messages++ >= max_messages) {
          return;
        }
        return done(null, 'fake message', fakeAckMessage);
      });

      var interval      = 1000;
      var past_interval = interval + 10;
      listen_options    = {
        max_outstanding: 2,
        lock_time:       3000,
        interval:        interval,
      };
      queue.listen('a queue', listen_options, consumer);

      clock.tick(past_interval);
      expect(queue.consume).to.have.been.calledOnce();
      expect(queue.consume).to.have.been.calledWith('a queue', { count: 2, lock_time: 3000, });
      queue.consume.reset();

      clock.tick(100);

      expect(queue.consume).not.to.have.been.called();

      clock.tick(past_interval);

      expect(queue.consume).to.have.been.calledOnce();
      expect(queue.consume).to.have.been.calledWith('a queue', { count: 1, lock_time: 3000, });
      queue.consume.reset();

      clock.tick(past_interval);

      expect(fakeAckMessage).to.have.callCount(1);

      expect(queue.consume).not.to.have.been.called();

      clock.tick(3000);
      expect(fakeAckMessage).to.have.callCount(2);

      clock.tick(4000);
      expect(fakeAckMessage).to.have.callCount(3);

      clock.tick(30000);
      expect(fakeAckMessage).to.have.callCount(5);

      return done();
    });

    context('when there are no messages', function() {
      it('does not call the consumer', function(done) {
        var clock    = this.sinon.useFakeTimers();
        var consumer = this.sinon.spy();

        this.sinon.stub(queue, 'consume', function(queue_name, consume_options, done) {
          return done();
        });

        listen_options    = {
          max_outstanding: 2,
          lock_time:       3000,
          interval:        100,
        };
         queue.listen('an empty queue', listen_options, consumer);

         clock.tick(30000);

         expect(consumer).to.have.callCount(0);

         return done();
      });
    });
  });

  describe('#size', function() {
    var queue;

    beforeEach(function(done) {
      DBQueue.connect(helper.test_db_config, function(err, result) {
        expect(err).to.not.exist();

        queue = result;

        return done();
      });
    });

    context('when there aren\'t any jobs', function() {
      it('returns 0', function(done) {
        queue.size('queue_a', function(err, count) {
          expect(err).to.not.exist();

          expect(count).to.equal(0);

          return done();
        });
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

      it('returns the number of jobs in the queue', function(done) {
        queue.size('queue_a', function(err, count) {
          expect(err).to.not.exist();

          expect(count).to.equal(1);

          return done();
        });
      });

      context('when multiple queues are requested', function() {
        it('returns the total number of jobs across the queues', function(done) {
          queue.size(['queue_a', 'queue_b'], function(err, count) {
            expect(err).to.not.exist();

            expect(count).to.equal(2);

            return done();
          });
        });
      });
    });
  });

  describe('integration tests', function() {
    describe('custom table name support', function() {
      var queue;

      beforeEach(function(done) {
        var custom_config = _.extend({}, helper.test_db_config, {
          table_name: 'custom_jobs_table',
        });
        DBQueue.connect(custom_config, function(err, result) {
          expect(err).to.not.exist();

          queue = result;

          return done();
        });
      });

      context('when provided a custom table name', function() {
        it('uses the provided table name', function(done) {
          queue.insert('custom_table_queue', 'fake data for custom table queue', function(err) {
            expect(err).to.not.exist();

            queue.size('custom_table_queue', function(err, size) {
              expect(err).to.not.exist();

              expect(size).to.equal(1);

              db.query('SELECT * FROM jobs', function(err, rows) {
                expect(err).to.not.exist();

                expect(rows).to.deep.equal([]);

                db.query('SELECT * FROM custom_jobs_table', function(err, rows) {
                  expect(err).to.not.exist();

                  expect(rows).to.have.length(1);

                  queue.consume('custom_table_queue', function(err, job, completionCallback) {
                    expect(err).to.not.exist();

                    expect(job).to.exist();
                    expect(job).to.equal('fake data for custom table queue');

                    completionCallback(null);
                    setTimeout(function () {
                      db.query('SELECT * FROM custom_jobs_table', function(err, rows) {
                        expect(err).to.not.exist();
                        expect(rows).to.have.length(0);
                        return done();
                      });
                    }, 10);

                  });
                });
              });
            });
          });
        });
      });
    });

    describe('serialization support', function() {
      context('when provided serializer/deserializer functions', function() {
        it('uses those functions to serialize/deserialize job data', function(done) {
          var options = _.extend({}, helper.test_db_config, {
            serializer:   yaml.dump,
            deserializer: yaml.load,
          });

          var queue = new DBQueue(options);

          queue.insert('a queue', { fake: 'job data' }, function(err) {
            expect(err).to.not.exist();

            db.query('SELECT data FROM jobs', function(err, rows) {
              expect(err).to.not.exist();

              expect(rows).to.deep.equal([
                {
                  data: 'fake: job data\n'
                },
              ]);

              queue.consume('a queue', function(err, data, ackCallback) {
                expect(err).to.not.exist();

                expect(data).to.deep.equal({
                  fake: 'job data',
                });

                return done();
              });
            });
          });
        });
      });

      context('when not provided a serializer/deserializer', function() {
        it('defaults to JSON.stringify/JSON.parse', function(done) {
          var queue = new DBQueue(helper.test_db_config);

          queue.insert('a queue', { fake: 'job data' }, function(err) {
            expect(err).to.not.exist();

            db.query('SELECT data FROM jobs', function(err, rows) {
              expect(err).to.not.exist();

              expect(rows).to.deep.equal([
                {
                  data: '{"fake":"job data"}'
                },
              ]);

              queue.consume('a queue', function(err, data, ackCallback) {
                expect(err).to.not.exist();

                expect(data).to.deep.equal({ fake: 'job data' });

                return done();
              });
            });
          });
        });
      });

      context('when serialization fails', function() {
        it('yields an error', function(done) {
          var options = _.extend({}, helper.test_db_config, {
            serializer: function(data) {
              return data.someInvalidMethod();
            },
          });

          var queue = new DBQueue(options);

          queue.insert('a queue', { fake: 'job data' }, function(err) {
            expect(err).to.exist();

            return done();
          });
        });
      });

      context('when deserialization fails', function() {
        it('yields an error', function(done) {
          var options = _.extend({}, helper.test_db_config, {
            serializer: function(data) {
              return data;
            },
          });

          var queue = new DBQueue(options);

          queue.insert('a queue', 'an invalid json string', function(err) {
            expect(err).to.not.exist();

            db.query('SELECT data FROM jobs', function(err, rows) {
              expect(err).to.not.exist();

              expect(rows).to.deep.equal([
                {
                  data: 'an invalid json string'
                },
              ]);

              queue.consume('a queue', function(err, data, ackCallback) {
                expect(err).to.exist();

                return done();
              });
            });
          });
        });
      });
    });
  });
});
