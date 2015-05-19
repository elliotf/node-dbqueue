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

module.exports = Queue;
