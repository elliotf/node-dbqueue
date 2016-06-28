'use strict';

var mysql = require('mysql2');
var uuid  = require('uuid');

function DBQueue(attrs) {
  this.db     = attrs.db;
  this.table  = attrs.table || 'jobs';
  this.worker = uuid.v4();
}

DBQueue.connect = function(options, done) {
  var pool = mysql.createPool(options);
  pool.on('connection', function(conn) {
    conn.query('SET sql_mode="STRICT_ALL_TABLES"', [])
  });

  var queue   = new DBQueue({
    db:    pool,
    table: options.table_name,
  });

  return done(null, queue);
};

DBQueue.prototype.insert = function(queue_name, data, done) {
  var self  = this;
  var db    = this.db;
  var table = this.table;

  var sql = ""
    + " INSERT INTO ?? (queue, data, worker, create_time, update_time)"
    + " VALUES (?, ?, 'unassigned', NOW(), NOW())"
    ;
  db.query(sql, [table, queue_name, data], function(err, rows, fields) {
    if (err) {
      return done(err);
    }

    return done();
  });
};

DBQueue.prototype.consume = function(queue_input, done) {
  var db        = this.db;
  var table     = this.table;
  var worker_id = this.worker
  var self      = this;
  var lock_time = 60 * 15;

  var free_job_ids_sql = ""
    + " SELECT *"
    + " FROM ??"
    + " WHERE locked_until < NOW()"
    + " AND queue IN (?)"
    + " ORDER BY RAND()"
    + " LIMIT 1" // eventually query/update batches to be easier on the DB
    ;
  db.query(free_job_ids_sql, [table, queue_input], function(err, rows) {
    if (err) {
      return done(err);
    }

    if (!rows.length) {
      return done();
    }

    var job_ids = rows.map(function(row) {
      return row.id;
    });

    var reserve_jobs_sql = ""
      + " UPDATE ??"
      + " SET"
      + "   worker = ?"
      + "   , locked_until = (NOW() + INTERVAL ? SECOND)"
      + "   , update_time = NOW()"
      + " WHERE id IN (?)"
      + " AND locked_until < NOW()"
      + " LIMIT 1"
      ;

    db.query(reserve_jobs_sql, [table, worker_id, lock_time, job_ids], function(err, result) {
      if (err) {
        return done(err);
      }

      var find_reserved_jobs_sql = ""
        + " SELECT *"
        + " FROM ??"
        + " WHERE id IN (?)"
        + " AND worker = ?"
        ;

      db.query(find_reserved_jobs_sql, [table, job_ids, worker_id], function(err, rows) {
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
