# DBQueue
A simple job queue that has priorities other than speed and scalability, inspired by TheSchwartz

# Usage

See usage in the tests, or see below example:

```
var DBQueue = require('dbqueue');

// use the included schema.sql to initialize the DB schema

var queue_options = {
  // node-mysql compatible DB settings
  host:             '127.0.0.1',
  user:             'root',
  password:         '',
  database:         'dbqueue_testing_db',
};

DBQueue.connect(queue_options, function(err, queue) {
  if (err) {
    // likely a DB connection error
  }

  var job_details = {
    example: 'job data',
  };

  // in your producer
  queue.insert('queue_name_here', JSON.stringify(job_details), function(err) {
    if (err) {
      // likely a DB connection error
    }

    // job enqueued, congratulations!
  });

  // in your consumer
  queue.consume('queue_name_here', function(err, job, finished) {
    if (err) {
    }

    if (!job) {
      // if there are no jobs on the queue
    }

    var job_data = JSON.parse(job);

    // do something with said job data

    // then let the queue know the job has been handled
    finished();

    // if you would like to get confirmation that the job has been cleared from the queue:
    finished(function(err) {
      if (err) {
        // job is likely still on the queue
      }
    });
  });
});

```

# When this might be a useful library
* When you don't want to introduce another dependency for simple/trivial functionality
* When you need a durable queue

# When this is NOT the solution for you
* You need guarantees that a job will be delivered once (your jobs are not idempotent)
* You need near-realtime performance
* You need to scale to large numbers of jobs

# Performance improvements
* fetch batches of jobs rather than one at a time
  * when #pop is called
    * and we have no items in the working batch
      * look for N jobs to work on
      * reserve them all
      * shift the first off and return it
    * and we *do* have items in the working batch
      * shift the first off and return it
      * reserve another N ?
  * so long as we can process the jobs quickly, this should be okay
    * but if we're too slow, we might have stale jobs that someone else is working on
