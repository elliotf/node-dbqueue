# theschwartz
A job queue has priorities other than speed and scalability

## Development TODO
* enqueue job
* dequeue job


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
