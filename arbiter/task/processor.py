#!/usr/bin/python
# coding=utf-8
""" Task process """
import logging
import multiprocessing

multiprocessing.set_start_method('fork')


class TaskProcess(multiprocessing.Process):
    """ Worker process for task """

    def __init__(self, settings, subscriptions, executable, task_name, task_key, result_queue, args, kwargs):
        super().__init__(target=executable)
        self.settings = settings
        self.subscriptions = subscriptions
        self.task_name = task_name
        self.task_key = task_key
        self.task_args = args
        self.task_kwargs = kwargs
        self.logger = logging
        self.result_queue = result_queue

    def run(self):
        """ Run worker process """
        self.logger = logging.getLogger(f"task.{self.task_key}")
        # Run
        try:
            # Execute code
            self.logger.info(f"Starting task - {self.task_key}")
            # importing code using settings passed from command line
            if self._target:
                self.result_queue.put(self._target(*self.task_args, **self.task_kwargs))
        except:  # pylint: disable=W0702
            self.logger.exception("Exception during running the task")
        finally:
            self.logger.info("Task exiting")
            for handler in logging.getLogger("").handlers:
                handler.flush()
