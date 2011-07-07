"""
Base Task

This is the Task interface that you have to subclass to create new workflow 
tasks.
"""
import logging


class Task(object):
    deps = ()
    logger = logging.getLogger('sworkflow')

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def execute(self):
        pass # placeholder

    def log(self, msg, *args, **kwargs):
        level = kwargs.pop('level', logging.INFO)
        self.logger.log(level, "[%s] %s" % (self, msg), *args, **kwargs)

    def __str__(self):
        return self.__class__.__name__

