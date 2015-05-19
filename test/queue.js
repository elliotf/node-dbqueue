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

    context.skip('when provided a transaction', function() {
      it('uses the transaction to insert the row', function(done) {
      });
    });
  });
});
