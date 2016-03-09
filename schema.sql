CREATE TABLE jobs (
  id           bigint unsigned not null auto_increment primary key,
  queue        varchar(255) not null,
  data         text default null,
  locked_until timestamp not null default '2001-01-01 00:00:00',
  worker       varchar(255) not null,
  create_time  timestamp not null default '2001-01-01 00:00:00',
  update_time  timestamp not null default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,

  KEY          ix_queue_locked_until (queue,locked_until),
  KEY          ix_locked_until (locked_until),
  KEY          ix_worker (worker)
);
