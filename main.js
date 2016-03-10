'use strict';

var mysql = require('mysql');
var uuid  = require('uuid');

function DBQueue(attrs) {
  this.db     = attrs.db;
  this.worker = uuid.v4();
}

DBQueue.connect = function(options, done) {
  var pool = mysql.createPool(options);
  pool.on('connection', function(conn) {
    conn.query('SET sql_mode="STRICT_ALL_TABLES"', [])
  });

  var queue   = new DBQueue({
    db: pool,
  });

  return done(null, queue);
};

DBQueue.prototype.insert = function(queue_name, data, done) {
  var self = this;
  var db   = this.db;

  var sql = ""
    + " INSERT INTO jobs (queue, data, worker, create_time, update_time)"
    + " VALUES (?, ?, 'unassigned', NOW(), NOW())"
    ;
  db.query(sql, [queue_name, data], function(err, rows, fields) {
    if (err) {
      return done(err);
    }

    return done();
  });
};

DBQueue.prototype.consume = function(queue_input, done) {
  var db        = this.db;
  var worker_id = this.worker
  var self      = this;
  var lock_time = 60 * 15;

  var queue_names = queue_input instanceof Array ? queue_input : [queue_input];
  var queue_ph    = queue_names.map(function() {
    return '?';
  }).join(',');

  var free_job_ids_sql = ""
    + " SELECT *"
    + " FROM jobs"
    + " WHERE locked_until < NOW()"
    + " AND queue IN (" + queue_ph + ")"
    + " ORDER BY RAND()"
    + " LIMIT 1" // eventually query/update batches to be easier on the DB
    ;
  db.query(free_job_ids_sql, queue_names, function(err, rows) {
    if (err) {
      return done(err);
    }

    if (!rows.length) {
      return done();
    }

    var job_ids = rows.map(function(row) {
      return row.id;
    });

    var placeholder_string = rows.map(function() {
      return '?';
    }).join(',');

    var reserve_jobs_sql = ""
      + " UPDATE jobs"
      + " SET"
      + "   worker = ?"
      + "   , locked_until = (NOW() + INTERVAL ? SECOND)"
      + "   , update_time = NOW()"
      + " WHERE id IN ("
      + placeholder_string
      + " )"
      + " AND locked_until < NOW()"
      + " LIMIT 1"
      ;

    var values = Array.prototype.concat.apply([worker_id, lock_time], job_ids);
    db.query(reserve_jobs_sql, values, function(err, result) {
      if (err) {
        return done(err);
      }

      var find_reserved_jobs_sql = ""
        + " SELECT *"
        + " FROM jobs"
        + " WHERE id IN ("
        + placeholder_string
        + " )"
        + " AND worker = ?"
        ;

      var values = Array.prototype.concat.apply(job_ids, [worker_id]);
      db.query(find_reserved_jobs_sql, values, function(err, rows) {
        if (err) {
          return done(err);
        }

        var job = rows[0];
        function finished(done) {
          var remove_job_sql = ""
            + " DELETE FROM jobs"
            + " WHERE id = ?"
            ;
          db.query(remove_job_sql, [job.id], function(err, result) {
            if (err) {
              return done(err);
            }

            if (done instanceof Function) {
              done();
            }
          });
        }

        return done(null, job, finished);
      });
    });
  });
};

DBQueue.prototype.size = function(queue_input, done) {
  var db          = this.db;
  var queue_names = queue_input instanceof Array ? queue_input : [queue_input];
  var queue_ph    = queue_names.map(function() {
    return '?';
  }).join(',');

  var total_jobs_sql = ""
    + " SELECT COUNT(1) AS total"
    + " FROM jobs"
    + " WHERE queue IN (" + queue_ph + ")"
    ;
  db.query(total_jobs_sql, queue_names, function(err, rows) {
    if (err) {
      return done(err);
    }

    var count = rows[0].total;

    return done(null, count);
  });
};

module.exports = DBQueue;
