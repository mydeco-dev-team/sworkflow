import sys
from subprocess import check_call, CalledProcessError

from .task import Task
from .workflow import ExitWorkflow


class PythonTask(Task):
    """Execute python modules

    class SampleProg(PythonTask):
        execargs = ['mymodule', '-arg', '1', '-arg2=3']
    """
    python_interpreter = sys.executable
    execargs = ()
    execenv = None
    execcwd = None
    cancelworkflow_retcode = ExitWorkflow.EXIT_CANCELLED

    def _run(self):
        assert self.execargs, 'missing execargs'
        args = (self.python_interpreter, '-m') + tuple(self.execargs)
        self.log('Running %s', ' '.join(args))
        check_call(args, env=self.execenv, cwd=self.execcwd)

    def execute(self):
        try:
            self._run()
        except CalledProcessError, exc:
            if ExitWorkflow.is_status(exc.returncode):
                raise ExitWorkflow(str(self), exc.returncode)
            raise
