import datetime
import json
import logging
import os
import threading
from datetime import datetime
from queue import Queue

from job import Job

logging.basicConfig(filename='scheduler_log.log',
                    filemode='a',
                    format='%(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
logger = logging.getLogger('mylog')


class Scheduler:
    def __init__(self, pool_size: int = 10):
        self.pool_size = pool_size
        self.jobs: list = []
        self.waiting_jobs: list = []
        self.completed_jobs: list = []
        self.queue: Queue = Queue()
        self.queue_size: int = 0
        self._stopped: bool = False
        self.functions_dict: dict = {}

    def schedule(self, job):
        if job.function_name not in self.functions_dict:
            self.functions_dict[job.function_name] = job.function
        if self.queue_size < self.pool_size:
            self.queue.put(job)
            self.queue_size += 1
            while self.waiting_jobs and self.queue_size <= self.pool_size:
                waiting_task = self.waiting_jobs.pop(0)
                self.queue.put(waiting_task)
                self.queue_size += 1
        else:
            if job not in self.waiting_jobs:
                self.waiting_jobs.append(job)

    def _run(self):
        while not self.queue.empty() and not self._stopped:
            job = self.queue.get()
            self.queue_size -= 1
            if job.dependencies and not all(
                    dep in self.completed_jobs for dep in job.dependencies):
                self.schedule(job)
                continue
            try:
                job.run()
                job.status = 'waiting'
                self.schedule(job)
            except StopIteration:
                self.completed_jobs.append(job)
                continue

    def run(self):
        logger.info('Запуск потока выполнения метода Scheduler._run()')
        t = threading.Thread(target=self._run)
        t.start()

    def stop(self):
        logger.info(f'Остановка работы {datetime.now()}')
        self._stopped = True
        logger.info('Загрузка состояния в файл "scheduler_state.json"')
        self.save_statement()

    def restart(self):
        logger.info(f'Перезапуск {datetime.now()}')
        self._stopped = False
        self.load_statement()
        self._run()

    def save_statement(self):
        with open('scheduler_state.json', 'w') as f:
            state = {
                'pool_size': self.pool_size,
                'jobs': [job.to_dict() for job in self.jobs],
                'waiting_jobs': [job.to_dict() for job in self.waiting_jobs],
                'completed_jobs': [job.to_dict() for job in
                                   self.completed_jobs],
                'queue_size': self.queue_size,
            }
            json.dump(state, f)

    def load_statement(self):
        with open('scheduler_state.json', 'r') as f:
            state = json.load(f)
        self.pool_size = state['pool_size']
        self.jobs = [Job.from_dict(job_dict, self.functions_dict) for job_dict
                     in state['jobs']]
        self.waiting_jobs = [Job.from_dict(job_dict, self.functions_dict) for
                             job_dict in state['waiting_jobs']]
        self.completed_jobs = [Job.from_dict(job_dict, self.functions_dict) for
                               job_dict in state['completed_jobs']]
        self.queue_size = state['queue_size']
        for job in self.jobs + self.waiting_jobs:
            if job.status in ('waiting', 'running', 'pending'):
                self.queue.put(job)
        os.remove('scheduler_state.json')
