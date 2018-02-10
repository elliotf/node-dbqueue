'use strict';

var mysql = require('mysql2');
var uuid  = require('uuid');

function DBQueue(attrs) {
  this.table  = attrs.table_name || 'jobs';
  this.worker = uuid.v4();

  this.serializer   = attrs.serializer   || JSON.stringify;
  this.deserializer = attrs.deserializer || JSON.parse;

  var pool = mysql.createPool(attrs);
  pool.on('connection', function(conn) {
    conn.query('SET sql_mode="STRICT_ALL_TABLES"', [])
  });

  this.db = pool;
}

DBQueue.connect = function(options, done) {
  var queue   = new DBQueue(options);

  queue.query("SELECT NOW()", [], function(err, result) {
    if (err) {
      return done(err);
    }

    return done(null, queue);
  });
};

DBQueue.prototype.query = function(sql, bindings, done) {
  return this.db.getConnection(function(err, connection) {
    if (err) {
      return done(err);
    }

    connection.query(sql, bindings, function(err, results) {
      connection.release();

      if (err) {
        return done(err);
      }

      return done(err, results);
    });
  });
};

DBQueue.prototype.insert = function(queue_name, data, done) {
  var self  = this;
  var table = this.table;

  var to_store;
  try {
    to_store = this.serializer(data);
  } catch(e) {
    return done(e);
  }
  var sql = ""
    + " INSERT INTO ?? (queue, data, worker, create_time, update_time)"
    + " VALUES (?, ?, 'unassigned', NOW(), NOW())"
    ;
  this.query(sql, [table, queue_name, to_store], function(err, rows, fields) {
    if (err) {
      return done(err);
    }

    return done();
  });
};

function reserveJobs(queue, queue_input, options, done) {
  var self      = queue;
  var table     = queue.table;
  var worker_id = uuid.v4();

  var lock_time = options.lock_time || (60 * 5);
  var limit     = options.count     || 1;

  self.query("SELECT NOW() AS now, NOW() + INTERVAL ? SECOND AS lock_until", [lock_time], function(err, result) {
    if (err) {
      return done(err);
    }

    var now        = result[0].now;
    var lock_until = result[0].lock_until;

    var reserve_jobs_sql = ""
      + " UPDATE ??"
      + " SET"
      + "   worker = ?"
      + "   , locked_until = ?"
      + "   , update_time = ?"
      + " WHERE locked_until < ?"
      + " AND queue IN (?)"
      + " LIMIT ?"
      ;

    self.query(reserve_jobs_sql, [table, worker_id, lock_until, now, now, queue_input, limit], function(err, result) {
      if (err) {
        return done(err);
      }

      if (!result.affectedRows) {
        return done();
      }

      var find_reserved_jobs_sql = ""
        + " SELECT *"
        + " FROM ??"
        + " WHERE worker = ?"
        + " AND locked_until = ?"
        ;

      return self.query(find_reserved_jobs_sql, [table, worker_id, lock_until], done);
    });
  });
}

DBQueue.prototype.consume = function(queue_input, options_input, done_input) {
  var table = this.table;
  var self  = this;

  var options;
  var done;
  if (options_input && (typeof done_input === 'function')) {
    options = options_input;
    done    = done_input;
  } else {
    options = {};
    done    = options_input;
  }

  reserveJobs(this, queue_input, options, function(err, rows) {
    if (err) {
      return done(err);
    }

    if (!rows || !rows.length) {
      // not tested, but a potential race condition due to replication latency in multi-master setup
      // let's avoid an uncaught exception when we try to pull .data off of undefined
      return done();
    }

    rows.forEach(function(job) {
      function finishedWithJob(err) {
        if (err) {
          return;
        }

        self.query("DELETE FROM ?? WHERE id = ?", [table, job.id], function(err, result) {
          if (err) {
            console.error('Error acking message:', err, err.stack);
          }
        });
      }

      var to_return;
      try {
        to_return = self.deserializer(job.data);
      } catch(e) {
        return done(e);
      }

      return done(null, to_return, finishedWithJob);
    });
  });
};

DBQueue.prototype.listen = function(queue_name, options, consumer) {
  var interval        = options.interval        || 1000;
  var max_outstanding = options.max_outstanding || 1;
  var max_at_a_time   = options.max_jobs_per_interval || 0;
  var outstanding     = 0;

  var timer = setInterval(function() {
    var num_to_consume = max_outstanding - outstanding;

    if (!num_to_consume) {
      return;
    }

    if (max_at_a_time) {
      num_to_consume = Math.min(num_to_consume, max_at_a_time);
    }

    var consume_options = {
      lock_time: options.lock_time,
      count:     num_to_consume,
    };

    this.consume(queue_name, consume_options, function(err, message, ackMessage) {
      if (err) {
        return;
      }

      if (!message) {
        return;
      }

      outstanding++;
      consumer(null, message, function(err) {
        ackMessage(err);

        outstanding--;
      });
    });
  }.bind(this), interval);

  function stop() {
    clearInterval(timer);
  }

  return stop;
};

DBQueue.prototype.size = function(queue_input, done) {
  var table = this.table;

  var total_jobs_sql = ""
    + " SELECT COUNT(1) AS total"
    + " FROM ??"
    + " WHERE queue IN (?)"
    ;
  this.query(total_jobs_sql, [table, queue_input], function(err, rows) {
    if (err) {
      return done(err);
    }

    var count = rows[0].total;

    return done(null, count);
  });
};

module.exports = DBQueue;
