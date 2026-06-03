# grader

## Request queue for grading

The grading API now uses an internal FIFO queue. Incoming `/grade` / `/api/grade` requests are accepted, queued, and processed by background worker threads.

- Default behavior: sequential grading (`GRADE_QUEUE_WORKERS=1`)
- API contract stays synchronous: each request waits for its own queued job to finish and then returns the same grading JSON response format as before.
- If queue capacity is exhausted, API returns `503` (system busy).
- If waiting for queued completion exceeds timeout, API returns `504`.
  - Note: the queued grading job may still continue in the background and be cleaned up by the worker.

## Isolation under load

Each request is saved and processed inside a unique job directory:

- `app/tmp/queue_jobs/<job_id>/...`

This prevents concurrent requests from overwriting each other’s uploaded file/work artifacts.

## Environment variables

- `GRADE_QUEUE_WORKERS` (default: `1`): number of queue workers.
- `GRADE_QUEUE_MAX_SIZE` (default: `100`): max queued jobs waiting in memory.
- `GRADE_QUEUE_ENQUEUE_TIMEOUT_SECONDS` (default: `5`): max wait when enqueuing before returning `503`.
- `GRADE_QUEUE_RESULT_TIMEOUT_SECONDS` (default: `600`): max client wait for queued job completion before returning `504`.
- `GRADE_CLEANUP_ARTIFACTS` (`true`/`false`, default: `true`): cleanup grader artifacts after processing.

## Operational notes

- For safest operation with this legacy grader implementation, keep `GRADE_QUEUE_WORKERS=1`.
- Increase workers only if your grading pipeline is verified to be concurrency-safe.
