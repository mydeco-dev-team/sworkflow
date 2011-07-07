import sys
from subprocess import check_call, CalledProcessError

from .task import Task
from .workflow import CancelWorkflow, CANCELWORKFLOW


class PythonTask(Task):
    """Execute python modules

    class SampleProg(PythonTask):
        execargs = ['mymodule', '-arg', '1', '-arg2=3']
    """
    python_interpreter = sys.executable
    execargs = ()
    execenv = None
    execcwd = None
    cancelworkflow_retcode = CANCELWORKFLOW

    def execute(self):
        assert self.execargs, 'missing execargs'
        args = (self.python_interpreter, '-m') + tuple(self.execargs)
        self.log('Running %s', ' '.join(args))
        try:
            check_call(args, env=self.execenv, cwd=self.execcwd)
        except CalledProcessError, exc:
            if exc.returncode == self.cancelworkflow_retcode:
                raise CancelWorkflow()
            raise
