import os
import time

from job import Job
from scheduler import Scheduler
from utils import (test_function_1, test_function_2, test_function_3,
                   get_list_by_url, BASE_DIR, analytics, delete_directory)


def dependent_tasks():
    dependent_value = 4

    job1 = Job(target=test_function_1, data_list=[(dependent_value,)])
    job2 = Job(target=test_function_2,
               data_list=[tuple()],
               dependencies=[job1])
    job3 = Job(target=test_function_3,
               data_list=[tuple()],
               dependencies=[job1, job2])

    schedule = Scheduler()
    schedule.schedule(job1)
    schedule.schedule(job2)
    schedule.schedule(job3)
    schedule.run()


def data_acquisition():
    job1 = Job(
        target=get_list_by_url,
        data_list=[
            ('https://code.s3.yandex.net/async-module/moscow-response.json',),
            ('https://code.s3.yandex.net/async-module/paris-response.json',),
            ('https://code.s3.yandex.net/async-module/london-response.json',),
        ])

    schedule = Scheduler()
    schedule.schedule(job1)
    schedule.run()
    time.sleep(2)


def data_analytics():
    downloads_dir = BASE_DIR / 'output_data'
    files = os.listdir(downloads_dir)
    job1 = Job(
        target=analytics,
        data_list=[(os.path.join(downloads_dir, file),) for file in files])
    job2 = Job(
        target=delete_directory,
        data_list=[(downloads_dir,)],
        dependencies=[job1]
    )
    schedule = Scheduler()
    schedule.schedule(job1)
    schedule.schedule(job2)
    schedule.run()


def stop_and_restart():
    schedule = Scheduler()
    for i in range(1, 2001):
        job = Job(target=print, data_list=[(i,), (i ** 2,)])
        schedule.schedule(job)
    schedule.run()
    schedule.stop()
    print('5 seconds relax for terminal)')
    time.sleep(5)
    schedule.restart()


if __name__ == '__main__':
    job1 = Job(target=dependent_tasks, data_list=[tuple()])
    job2 = Job(target=data_acquisition, data_list=[tuple()],
               dependencies=[job1])
    job3 = Job(target=data_analytics, data_list=[tuple()],
               dependencies=[job1, job2])
    job4 = Job(target=stop_and_restart, data_list=[tuple()],
               dependencies=[job1, job2, job3])
    schedule = Scheduler()
    schedule.schedule(job1)
    schedule.schedule(job2)
    schedule.schedule(job3)
    schedule.schedule(job4)
    schedule.run()
