import os
import time
import unittest

from job import Job
from scheduler import Scheduler
from utils import (test_function_4, test_function_5, test_function_6,
                   test_function_7, test_function_8)


class TestScheduler(unittest.TestCase):
    def setUp(self):
        self.scheduler = Scheduler()

    def test_scheduler_stop_restart(self):
        for i in range(1, 500):
            job = Job(target=test_function_4, data_list=[(4,)])
            self.scheduler.schedule(job)
        self.scheduler.run()
        self.scheduler.stop()
        self.assertTrue(self.scheduler._stopped)
        self.assertTrue(os.path.isfile(self.scheduler.statement_path))
        self.scheduler.restart()
        self.assertFalse(self.scheduler._stopped)
        self.assertFalse(os.path.isfile(self.scheduler.statement_path))

    def test_job_result(self):
        job = Job(target=test_function_4, data_list=[(4,)])
        self.scheduler.schedule(job)
        self.scheduler.run()
        self.assertEqual(job.result[0], 8)

    def test_job_restarts(self):
        job = Job(target=test_function_5, data_list=[tuple()], restarts=3)
        self.scheduler.schedule(job)
        self.scheduler.run()
        time.sleep(1)
        self.assertEqual(job.restarts, 0)

    def test_job_dependencies(self):
        dependent_value = 4
        job1 = Job(target=test_function_6, data_list=[(dependent_value,)])
        job2 = Job(target=test_function_7,
                   data_list=[tuple()],
                   dependencies=[job1])
        job3 = Job(target=test_function_8,
                   data_list=[tuple()],
                   dependencies=[job1, job2])
        check_data = [job1, job2, job3]
        self.scheduler.schedule(job3)
        self.scheduler.schedule(job2)
        self.scheduler.schedule(job1)
        self.scheduler.run()
        time.sleep(1)
        for ind, job_object in enumerate(check_data):
            self.assertEqual(job_object, self.scheduler.completed_jobs[ind])


if __name__ == "__main__":
    unittest.main()
