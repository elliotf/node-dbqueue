function Queue() {
}

Queue.prototype.enqueue = function(job_type, job, done) {
  done();
};

module.exports = Queue;
