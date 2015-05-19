var helper = require('./helper');
var Queue  = require('../');
var db     = helper.knex;
var expect = helper.expect;

describe('Queue', function() {
  describe('Constructor', function() {
    it('can be instantiated', function() {
      var queue = new Queue(db);
    });
  });

  describe('#enqueue', function() {
    var queue;

    beforeEach(function() {
      queue = new Queue(db);
    });

    it('records the job in the DB', function(done) {
      var job_data = {
        example: 'job metadata'
      };

      queue
        .enqueue('job type here', job_data)
        .asCallback(function(err) {
          expect(err).to.not.exist();

          db
            .select()
            .from('jobs')
            .asCallback(function(err, jobs) {
              expect(err).to.not.exist();

              expect(jobs).to.have.length(1);

              expect(jobs).to.deep.equal([
                {
                  id:        jobs[0].id,
                  job_type:  'job type here',
                  data:      '{"example":"job metadata"}',
                  locked_by: null
                }
              ]);

              expect(jobs[0].id).to.be.a('number');

              done();
            });
      });
    });

    context.skip('when provided a transaction', function() {
      it('uses the transaction to insert the row', function(done) {
      });
    });
  });

  describe('#acquire', function() {
    var queue;

    beforeEach(function() {
      queue = new Queue(db);
    });

    context('when there are no jobs', function() {
      it('yields null', function(done) {
        queue
          .acquire('example worker identifier')
          .asCallback(function(err, job) {
            expect(err).to.not.exist();

            expect(job).to.not.exist();

            done();
          });
      });
    });

    context('when there are jobs', function() {
      var jobs;

      beforeEach(function(done) {
        var rows = [
          {
            data:      '{"example": "job 0"}',
            job_type:  'example job type',
            locked_by: 'some other worker'
          },
          {
            data:      '{"example": "job 1"}',
            job_type:  'example job type'
          },
          {
            data:      '{"example": "job 2"}',
            job_type:  'example other job type'
          }
        ];

        db
          .insert(rows)
          .into('jobs')
          .asCallback(function(err) {
            expect(err).to.not.exist();

            db
              .select()
              .from('jobs')
              .orderBy('id ASC')
              .asCallback(function(err, rows) {
                expect(err).to.not.exist();

                jobs = rows;

                done();
              });
          });
      });

      it('marks a job as being claimed', function(done) {
        queue
          .acquire('example worker identifier')
          .asCallback(function(err, job) {
            expect(err).to.not.exist();

            db
              .select()
              .from('jobs')
              .orderBy('id ASC')
              .asCallback(function(err, actual) {
                expect(err).to.not.exist();

                expect(actual).to.be.an('array');
                actual.forEach(function(row) {
                  expect(row.id).to.be.a('number');
                  delete row.id;
                });

                expect(actual).to.deep.equal([
                  {
                    data:      '{"example": "job 0"}',
                    job_type:  'example job type',
                    locked_by: 'some other worker'
                  },
                  {
                    data:      '{"example": "job 1"}',
                    job_type:  'example job type',
                    locked_by: 'example worker identifier'
                  },
                  {
                    data:      '{"example": "job 2"}',
                    job_type:  'example other job type',
                    locked_by: null
                  }
                ]);

                expect(job.data).to.deep.equal(actual[1].data);

                done();
              });
        });
      });
    });

    context('when a transaction is provided', function() {
      it.skip('is used', function(done) {
      });
    });
  });
});
