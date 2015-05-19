/*
* schema
  * job_types
    * id
    * name
  * jobs
    * id
    * job_type_id ?
    * data
    * locked_by
    * status?
    * state?
*/

exports.up = function(knex, Promise) {
  var todo = [];

  todo.push(knex.schema.createTable('jobs', function(table) {
    table.bigIncrements('id').notNull();
    table.string('job_type').nullable();
    table.string('locked_by').nullable();
    table.text('data').nullable();
  }));

  return Promise.all(todo);
};

exports.down = function(knex, Promise) {
};
