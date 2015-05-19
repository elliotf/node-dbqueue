var helper = require('./helper');
var Queue  = require('../');
var db     = helper.knex;
var expect = helper.expect;

describe('Queue', function() {
  it('can be instantiated', function() {
    var queue = new Queue(db);
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
                  id:       jobs[0].id,
                  job_type: 'job type here',
                  data:     '{"example":"job metadata"}'
                }
              ]);

              expect(jobs[0].id).to.be.a('number');

              done();
            });
      });
    });

    context('when the job type does not exist', function() {
    });

    context('when the job type already exists', function() {
    });

    context.skip('when provided a transaction', function() {
      it('uses the transaction to insert the row', function(done) {
      });
    });
  });

  describe('#dequeue', function() {
    // pull a job off the queue
    // mark it as taken by a worker
    context('when a job type is provided', function() {
    });
  });

  describe('#requeue', function() {
    // if a worker chooses to stop working on a job
    // surrender it back to the queue
  });
});
