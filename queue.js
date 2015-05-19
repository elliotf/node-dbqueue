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

Queue.prototype.acquire = function(worker_identifier) {
  var db = this.db;

  // TODO: if we have some rows left in our cache from a previous query, use those

  return db
    .select()
    .from('jobs')
    .whereNull('locked_by')
    .orderBy('id ASC')
    .then(function(rows) {
      if (!rows.length) {
        return null;
      }

      return db('jobs')
        .update({ locked_by: worker_identifier })
        .where({ id: rows[0].id })
        .whereNull('locked_by')
        .then(function(rows_updated) {

          // TODO: if we updated the row
          return rows[0];
        });
    });
};

module.exports = Queue;
