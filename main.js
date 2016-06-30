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

  queue.db.query("SELECT NOW()", function(err, result) {
    if (err) {
      return done(err);
    }

    return done(null, queue);
  });
};

DBQueue.prototype.insert = function(queue_name, data, done) {
  var self  = this;
  var db    = this.db;
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
  db.query(sql, [table, queue_name, to_store], function(err, rows, fields) {
    if (err) {
      return done(err);
    }

    return done();
  });
};

DBQueue.prototype.consume = function(queue_input, done) {
  var db        = this.db;
  var table     = this.table;
  var worker_id = uuid.v4();
  var self      = this;
  var lock_time = 60 * 5;

  db.query("SELECT NOW() AS now, NOW() + INTERVAL ? SECOND AS lock_until", [lock_time], function(err, result) {
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
      + " LIMIT 1"
      ;

    db.query(reserve_jobs_sql, [table, worker_id, lock_until, now, now, queue_input], function(err, result) {
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

      db.query(find_reserved_jobs_sql, [table, worker_id, lock_until], function(err, rows) {
        if (err) {
          return done(err);
        }

        var job = rows[0];

        function finishedWithJob(err, done) {
          if (!(done instanceof Function)) {
            done = function() {};
          }

          if (err) {
            return done();
          }

          var remove_job_sql = ""
            + " DELETE FROM jobs"
            + " WHERE id = ?"
            ;
          db.query(remove_job_sql, [job.id], function(err, result) {
            if (err) {
              return done(err);
            }

            done();
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
  });
};

DBQueue.prototype.size = function(queue_input, done) {
  var db    = this.db;
  var table = this.table;

  var total_jobs_sql = ""
    + " SELECT COUNT(1) AS total"
    + " FROM ??"
    + " WHERE queue IN (?)"
    ;
  db.query(total_jobs_sql, [table, queue_input], function(err, rows) {
    if (err) {
      return done(err);
    }

    var count = rows[0].total;

    return done(null, count);
  });
};

module.exports = DBQueue;
