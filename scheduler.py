import datetime
import json
import os
import threading
from datetime import datetime
from queue import Queue

from config import SchedulerConfig
from job import Job
from logging_setting import logger


class Scheduler:
    def __init__(self, pool_size: int = None, statement_path: str = None):
        config = SchedulerConfig()
        self.pool_size = pool_size or config.pool_size
        self.statement_path = statement_path or config.statement_path
        self.jobs: list = []
        self.waiting_jobs: list = []
        self.completed_jobs: list = []
        self.queue: Queue = Queue()
        self.queue_size: int = 0
        self._stopped: bool = False
        self.functions_dict: dict = {}

    def schedule(self, job: Job):
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
                if job.status != 'error':
                    self.schedule(job)
                elif job.status == 'error':
                    if job.restarts == 0:
                        self.completed_jobs.append(job)
                        continue
                    while job.restarts > 0 and job.status == 'error':
                        job.restarts -= 1
                        job._run()
                        if job.status != 'error':
                            self.schedule(job)
                            break
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
        # В данном случае проверка на доступность statement_path не нужна,
        # так как здесь этот файл будет создаваться.
        with open(self.statement_path, 'w') as f:
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
        if not os.path.isfile(self.statement_path):
            raise FileNotFoundError(f'No such file: "{self.statement_path}"')
        with open(self.statement_path, 'r') as f:
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
        os.remove(self.statement_path)
