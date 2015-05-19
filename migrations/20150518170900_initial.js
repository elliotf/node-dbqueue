/*
* schema
  * job_types
    * id
    * name
  * jobs
    * id
    * job_type_id
    * locked_by
    * status
    * data
*/

exports.up = function(knex, Promise) {
  return knex.schema.createTable('jobs', function(table) {
    table.bigIncrements('id').notNull();
  });
};

exports.down = function(knex, Promise) {
};
