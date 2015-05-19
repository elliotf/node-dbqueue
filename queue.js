var uuid = require('uuid');

function Queue(db) {
  this.db = db;
}

Queue.prototype.enqueue = function(job_type, data) {
  var row = {
    data:     JSON.stringify(data),
    job_type: job_type
  };

  return this.db
    .insert(row)
    .into('jobs');
};

Queue.prototype.acquire = function() {
  var db      = this.db;
  var lock_id = uuid.v4();

  var free_job_query = db
    .select('id')
    .from('jobs')
    .whereNull('locked_by')
    .limit(1);

  return db('jobs')
    .update({ locked_by: lock_id })
    .where({ id: free_job_query })
    .then(function(rows_updated) {
      if (!rows_updated) {
        return null;
      }

      return db
        .select()
        .from('jobs')
        .where({ locked_by: lock_id })
        .then(function(rows) {
          return rows[0];
        });
    });
  // TODO: if we have some rows left in our cache from a previous query, use those

  return db
    .select()
    .from('jobs')
    .whereNull('locked_by')
    .orderBy('id ASC')
    .limit(100)
    .then(function(rows) {
      if (!rows.length) {
        return null;
      }

      return db('jobs')
        .update({ locked_by: worker_identifier })
        .where({ id: rows[0].id })
        .whereNull('locked_by')
        .then(function(rows_updated) {

          // TODO: try another row if we could not lock
          rows[0].locked_by = worker_identifier;
          return rows[0];
        });
    });
};

module.exports = Queue;
