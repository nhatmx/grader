import importlib
import os
import sys
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
APP_DIR = os.path.join(REPO_ROOT, 'app')
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)
os.chdir(APP_DIR)

os.environ.setdefault('GRADE_QUEUE_WORKERS', '1')
os.environ.setdefault('GRADE_QUEUE_MAX_SIZE', '100')
os.environ.setdefault('GRADE_QUEUE_RESULT_TIMEOUT_SECONDS', '30')
os.environ.setdefault('GRADE_QUEUE_ENQUEUE_TIMEOUT_SECONDS', '5')

app_module = importlib.import_module('app')


class QueueBehaviorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app_module.app)

    def setUp(self):
        self._original_grade = app_module.instructor_grade.instructor_grade_lab
        self._original_cleanup = app_module.instructor_grade.cleanup_submission_artifacts

    def tearDown(self):
        app_module.instructor_grade.instructor_grade_lab = self._original_grade
        app_module.instructor_grade.cleanup_submission_artifacts = self._original_cleanup

    def test_concurrent_requests_are_serialized_and_isolated(self):
        lock = threading.Lock()
        active_workers = 0
        max_active_workers = 0
        observed_paths = []

        def fake_grade(filepath):
            nonlocal active_workers, max_active_workers
            with lock:
                active_workers += 1
                max_active_workers = max(max_active_workers, active_workers)
                observed_paths.append(filepath)
            time.sleep(0.1)
            with lock:
                active_workers -= 1
            return {'student1.tcpip': {'grades': {'task_a': True, 'task_b': False}}}

        app_module.instructor_grade.instructor_grade_lab = fake_grade
        app_module.instructor_grade.cleanup_submission_artifacts = lambda *args, **kwargs: None

        def submit_request():
            return self.client.post(
                '/api/grade',
                files={'file': ('student1.tcpip.lab', b'dummy', 'application/octet-stream')},
            )

        with ThreadPoolExecutor(max_workers=5) as executor:
            responses = list(executor.map(lambda _: submit_request(), range(5)))

        self.assertTrue(all(resp.status_code == 200 for resp in responses))
        self.assertEqual(max_active_workers, 1)
        self.assertEqual(len(observed_paths), 5)
        self.assertEqual(len(set(observed_paths)), 5)
        self.assertTrue(
            all(
                os.path.join('tmp', 'queue_jobs') in path
                for path in observed_paths
            )
        )


if __name__ == '__main__':
    unittest.main()
