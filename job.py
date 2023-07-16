from typing import Any, Callable, Union


class Job:
    def __init__(self, target: Union[Callable, str],
                 data_list: list[tuple[Any]],
                 dependencies: list['Job'] = None):
        self.dependencies = dependencies if dependencies else []
        self.status: str = 'pending'
        self.data_list = data_list
        self.completed_data: list = []
        self.uncompleted_data: list = []
        self.function = target
        self.function_name = target.__name__
        self._generator = self._run()
        self.result: list = []

    def _run(self):
        for arg in self.data_list:
            self.status = 'running'
            value = self.function(*arg)
            if value is not None:
                self.result.append(value)
            self.completed_data.append(arg)
            self.status = 'waiting'
            yield

    def run(self):
        try:
            return next(self._generator)
        except StopIteration:
            self.status = 'completed'
            raise StopIteration

    def to_dict(self):
        self.uncompleted_data = [argument for argument in self.data_list if
                                 argument not in self.completed_data]
        return {
            'status': self.status,
            'dependencies': self.dependencies,
            'data_list': self.uncompleted_data,
            'completed_data': self.completed_data,
            'function_name': self.function_name,
            'result': self.result,
        }

    @staticmethod
    def from_dict(data, functions_dict):
        job = Job(
            target=functions_dict[data['function_name']],
            data_list=data['data_list'],
            dependencies=data['dependencies'],
        )
        job.status = data['status']
        job.completed_data = data['completed_data']
        job.completed_data = data['result']
        return job
