# DBQueue
A simple job queue that prioritizes infrastructural simplicity and common requirements over speed and scalability, inspired by TheSchwartz

# Usage

See usage in the tests, or see below example:

## Overview

```javascript
var DBQueue = require('dbqueue');

// use the included schema.sql to initialize the DB schema

var queue_options = {
  // node-mysql compatible DB settings
  host:             '127.0.0.1',
  port:             3306, // optional, defaults to 3306
  user:             'root',
  table_name:       'custom_jobs_table', // optional, defaults to `jobs`
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
    // passing an err to the finished callback will leave the job on the queue
    finished(some_err);

    // or if you would like to get confirmation that the job has been cleared from the queue:
    finished(null, function(err) {
      if (err) {
        // job is likely still on the queue
      }
    });
  });
});

```

## Connecting

Connect asynchronously to discover connectivity issues as soon as possible:
```javascript
var queue_options = {
  // options as above
};

DBQueue.connect(queue_options, function(err, queue) {
  if (err) {
    // likely a DB connection error
  }

  // start using queue
});
```

Connect lazily for less boilerplate
```javascript
var queue_options = {
  // options as above
};

var queue = new DBQueue(queue_options);

// start using queue, if there is a connection problem all queries are going to fail
```

## Inserting messages into the queue

```javascript
var queue_name   = 'example queue';
var message_data = { example: 'message data' };

queue.insert(queue_name, message_data, function(err) {
  // message_data is serialized to JSON by default
});
```

## Consuming messages from the queue

Message consumption currently reserves the message for five minutes.  If the message is not ACK'ed within that time, the message may be processed by another worker.

A customizable reservation time is a forthcoming feature.

```javascript
var queue_name   = 'example queue';
queue.consume(queue_name, function(err, message_data, ackMessageCallback) {
  // handle potential error
  // message data is thawed JSON by default
  // message data may be NULL if there are no available jobs on the queue
});
```

### ACK'ing and NACK'ing messages

Calling the ackMessageCallback without an error will remove it from the queue.

Calling the ackMessageCallback with an error will leave it on the queue to be processed again after some time.

Not calling the ackMessageCallback will leave it on the queue to be processed again after some time.

```javascript
var queue_name = 'example queue';
queue.consume(queue_name, function(err, message_data, ackMessageCallback) {
  // handle potential error

  // do something with the message, calling ackMessageCallback with the result
  // if ackMessageCallback is called with an error, the message is left on the queue
  // if the ackMessageCallback is not called, the message is left on the queue

  doSomethingWithMessage(message_data, function(err) {
    ackMessageCallback(err, function(err) {
      // handle potential error
      // callback to message ACK is optional, in case confirmation of message removal from queue is desired
    });
  });
});
```

## Custom serialization

In case you would like something other than JSON.stringify and JSON.parse for serialization, provide your own serialization methods.

Note that binary formats are currently not supported.

```javascript
var yaml = require('js-yaml');

var queue_options = {
  // ... options as before
  serializer:       yaml.dump,
  deserializer:     yaml.load,
};

var queue = new DBQueue(queue_options);
```

# When this might be a useful library
* You don't want to introduce another dependency for simple/trivial functionality
* You need a simple, durable queue
* You are okay with at least once semantics
* You would like message deferral without dead letter queue complexity

# When this is NOT the solution for you
* You need guarantees that a job will be delivered once and _only_ once (your jobs are not idempotent)
* You need near-realtime performance
* You need to scale to large numbers of jobs and/or very high throughput

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
