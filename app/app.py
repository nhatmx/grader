import logging
import os
import queue
import shutil
import threading
import uuid

from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse

import instructor_grade

app = FastAPI()
logger = logging.getLogger(__name__)

# === CONFIG ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'tmp', 'labs')
QUEUE_JOB_FOLDER = os.path.join(BASE_DIR, 'tmp', 'queue_jobs')
MAX_CONTENT_LENGTH = 10 * 1024 * 1024  # 10 MB
ALLOWED_EXTENSIONS = {'lab', 'zib'}

def _parse_bool_env(value):
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized in {'1', 'true', 'yes', 'on'}:
        return True
    if normalized in {'0', 'false', 'no', 'off'}:
        return False
    return None


def _is_production_env():
    env_name = (os.getenv('APP_ENV') or os.getenv('FLASK_ENV') or os.getenv('ENV') or '').strip().lower()
    return env_name == 'production'


def _should_cleanup_artifacts():
    # Override cleanup behavior with env var: true/false
    configured = _parse_bool_env(os.getenv('GRADE_CLEANUP_ARTIFACTS'))
    if configured is not None:
        return configured
    # Default: always cleanup to prevent disk growth.
    # Disable explicitly with GRADE_CLEANUP_ARTIFACTS=false when debugging artifacts.
    return True


def _parse_positive_int_env(name: str, default_value: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default_value
    try:
        parsed = int(value)
    except ValueError:
        logger.warning('Invalid integer for %s=%s. Falling back to %s', name, value, default_value)
        return default_value
    if parsed < 1:
        logger.warning('Invalid non-positive value for %s=%s. Falling back to %s', name, value, default_value)
        return default_value
    return parsed


QUEUE_WORKER_COUNT = _parse_positive_int_env('GRADE_QUEUE_WORKERS', 1)
QUEUE_MAX_SIZE = _parse_positive_int_env('GRADE_QUEUE_MAX_SIZE', 100)
QUEUE_ENQUEUE_TIMEOUT_SECONDS = _parse_positive_int_env('GRADE_QUEUE_ENQUEUE_TIMEOUT_SECONDS', 5)
QUEUE_RESULT_TIMEOUT_SECONDS = _parse_positive_int_env('GRADE_QUEUE_RESULT_TIMEOUT_SECONDS', 600)

# Ensure upload directories exist
for d in [UPLOAD_FOLDER, QUEUE_JOB_FOLDER]:
    os.makedirs(d, exist_ok=True)


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def normalize_filename(filename: str | None) -> str:
    return os.path.basename(filename or '')


def _save_upload(file: UploadFile, filepath: str, max_bytes: int) -> bool:
    size = 0
    file.file.seek(0)
    with open(filepath, 'wb') as buffer:
        while True:
            chunk = file.file.read(1024 * 1024)
            if not chunk:
                break
            size += len(chunk)
            if size > max_bytes:
                return False
            buffer.write(chunk)
    return True


class GradeJob:
    def __init__(self, job_id: str, filepath: str, submission_key: str, job_dir: str):
        self.job_id = job_id
        self.filepath = filepath
        self.submission_key = submission_key
        self.job_dir = job_dir
        self.done_event = threading.Event()
        self.status_code = 500
        self.payload = {'error': 'Lỗi nội bộ khi xử lý hàng đợi.'}


def _cleanup_job_files(job: GradeJob):
    """Remove uploaded file and per-job workspace directory; log failures without raising."""
    if os.path.exists(job.filepath):
        try:
            os.remove(job.filepath)
        except Exception:
            logger.warning('Failed to remove uploaded file for job_id=%s path=%s', job.job_id, job.filepath)
    if os.path.isdir(job.job_dir):
        try:
            shutil.rmtree(job.job_dir)
        except Exception:
            logger.warning('Failed to remove job folder for job_id=%s path=%s', job.job_id, job.job_dir)


def _run_grading_pipeline(job: GradeJob) -> tuple[int, dict]:
    """Execute grading and normalize output as (status_code, response_payload)."""
    raw_result = instructor_grade.instructor_grade_lab(job.filepath)

    if raw_result == 'wrong_input_file':
        return 400, {
            'error': 'File không hợp lệ. Không tìm thấy lab tương ứng trong hệ thống.'
        }

    if raw_result == {} or raw_result is None:
        return 400, {
            'error': 'Không thể chấm điểm. File có thể bị lỗi hoặc không đúng định dạng.'
        }

    key = list(raw_result.keys())[0]
    sv_data = raw_result[key]
    grades = sv_data.get('grades', {})

    tasks = []
    completed_count = 0
    for task_name, raw_value in grades.items():
        if task_name.startswith('_') or task_name.startswith('cw_'):
            continue

        completed = False
        if isinstance(raw_value, bool):
            completed = raw_value
        elif isinstance(raw_value, int):
            completed = raw_value > 0

        if completed:
            completed_count += 1

        tasks.append({
            'task': task_name,
            'completed': completed
        })

    total_tasks = len(tasks)
    score = round(10 * completed_count / total_tasks, 1) if total_tasks > 0 else 0.0

    parts = key.split('.')
    lab_name = parts[-1] if parts else 'unknown'
    email = '.'.join(parts[:-1]) if len(parts) > 1 else 'unknown'

    return 200, {
        'email': email,
        'lab_name': lab_name,
        'score': score,
        'completed_tasks': completed_count,
        'total_tasks': total_tasks,
        'tasks': tasks,
    }


def _process_queued_job(job: GradeJob):
    """Queue worker entrypoint: cleanup, execute grader, capture errors, and finalize job state."""
    logger.info('Start grading job_id=%s submission_key=%s', job.job_id, job.submission_key)
    try:
        status_code, payload = _run_grading_pipeline(job)
        job.status_code = status_code
        job.payload = payload
        logger.info('Completed grading job_id=%s status_code=%s', job.job_id, status_code)
    except Exception:
        error_id = uuid.uuid4().hex
        logger.exception('Queue worker error while processing job_id=%s error_id=%s', job.job_id, error_id)
        job.status_code = 500
        job.payload = {
            'error': f'Lỗi khi xử lý. Mã lỗi: {error_id}. Vui lòng thử lại hoặc liên hệ hỗ trợ.'
        }
    finally:
        if _should_cleanup_artifacts():
            instructor_grade.cleanup_submission_artifacts(BASE_DIR, job.submission_key, tmp_root=job.job_dir)
        _cleanup_job_files(job)
        job.done_event.set()


class GradeQueue:
    """In-memory FIFO queue that dispatches grading jobs to background worker threads."""

    def __init__(self, worker_count: int, max_size: int):
        self.queue = queue.Queue(maxsize=max_size)
        self.worker_count = worker_count
        self._start_workers()

    def _start_workers(self):
        for index in range(self.worker_count):
            worker = threading.Thread(target=self._worker_loop, args=(index + 1,), daemon=True)
            worker.start()
            logger.info('Started grade queue worker index=%s', index + 1)

    def _worker_loop(self, worker_index: int):
        while True:
            job = self.queue.get()
            logger.info('Dequeued job_id=%s on worker=%s queue_size=%s', job.job_id, worker_index, self.queue.qsize())
            try:
                _process_queued_job(job)
            finally:
                self.queue.task_done()

    def submit(self, job: GradeJob, timeout_seconds: int):
        self.queue.put(job, timeout=timeout_seconds)

    def qsize(self) -> int:
        return self.queue.qsize()


try:
    GRADE_QUEUE = GradeQueue(QUEUE_WORKER_COUNT, QUEUE_MAX_SIZE)
except Exception:
    logger.exception('Failed to initialize grading queue workers')
    raise


# === ROUTES ===

# @app.route('/')
# def index():
#     """Serve the grading web UI."""
#     return render_template('index.html')

# upload file API
@app.post('/grade')
@app.post('/api/grade')
def grade_lab(file: UploadFile | None = File(default=None)):
    """
    Accepts a .lab/.zib file upload, runs the grading pipeline,
    and returns a clean JSON result for the student.
    """
    # 1. Validate file
    if file is None:
        return JSONResponse({'error': 'Không tìm thấy file trong request.'}, status_code=400)

    filename = normalize_filename(file.filename)

    if filename == '':
        return JSONResponse({'error': 'Chưa chọn file nào.'}, status_code=400)

    if not allowed_file(filename):
        return JSONResponse({'error': 'Định dạng không hỗ trợ. Vui lòng upload file .lab hoặc .zib'}, status_code=400)

    # 2. Save uploaded file to a per-request directory to avoid collisions.
    job_id = uuid.uuid4().hex
    job_dir = os.path.join(QUEUE_JOB_FOLDER, job_id)
    os.makedirs(job_dir, exist_ok=True)
    filepath = os.path.join(job_dir, filename)
    submission_key = os.path.splitext(filename)[0]

    saved = _save_upload(file, filepath, MAX_CONTENT_LENGTH)
    try:
        file.file.close()
    except Exception:
        pass

    if not saved:
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
            except Exception:
                pass
        if os.path.isdir(job_dir):
            try:
                shutil.rmtree(job_dir)
            except Exception:
                pass
        return JSONResponse({'error': 'File quá lớn.'}, status_code=413)

    job = GradeJob(job_id=job_id, filepath=filepath, submission_key=submission_key, job_dir=job_dir)
    try:
        GRADE_QUEUE.submit(job, timeout_seconds=QUEUE_ENQUEUE_TIMEOUT_SECONDS)
        logger.info(
            'Queued grading request job_id=%s submission_key=%s queue_size=%s',
            job_id,
            submission_key,
            GRADE_QUEUE.qsize(),
        )
    except queue.Full:
        _cleanup_job_files(job)
        logger.warning('Queue is full. Rejecting job_id=%s queue_size=%s', job_id, GRADE_QUEUE.qsize())
        return JSONResponse({
            'error': 'Hệ thống đang bận. Vui lòng thử lại sau ít phút.'
        }, status_code=503)

    completed = job.done_event.wait(timeout=QUEUE_RESULT_TIMEOUT_SECONDS)
    if not completed:
        logger.error('Timed out waiting for queue result job_id=%s timeout=%s', job_id, QUEUE_RESULT_TIMEOUT_SECONDS)
        return JSONResponse({
            'error': 'Hết thời gian chờ xử lý. Vui lòng thử lại sau.'
        }, status_code=504)

    return JSONResponse(job.payload, status_code=job.status_code)


if __name__ == '__main__':
    # Run on all interfaces so other machines can access
    import uvicorn

    uvicorn.run(app, host='0.0.0.0', port=5000)
