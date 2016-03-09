
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

# general workflow
```SQL
-- insert a job

INSERT INTO jobs (category, data) VALUES (?,?);

-- get a batch of jobs to work on

SELECT (NOW() + INTERVAL ? SECOND) AS lock_until

SELECT id
FROM jobs
WHERE locked_until < NOW()
AND category = ?
ORDER BY RAND()
LIMIT ?

UPDATE jobs
SET worker_identifier = ?, locked_until = ?
WHERE id IN (/* batch ids */)
AND locked_until < NOW()
LIMIT ?

SELECT *
FROM jobs
WHERE id IN (/* batch ids */)
AND worker_identifier = ?

-- once complete, delete rows

DELETE
FROM jobs
WHERE id IN (/* batch ids */)
AND worker_identifier = ?
```
