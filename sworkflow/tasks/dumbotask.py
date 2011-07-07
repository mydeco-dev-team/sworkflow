from time import sleep
from sworkflow import hdfs
from .pythontask import PythonTask

def job_succeeded(output_dir, _ls=hdfs.ls, allow_empty=False):
    """Check if dumbo job succeed looking at output dir content

    dumbo has not a nice way to detect when a hadoop job failed,
    this function looks for incosistent data in output_dir.

    it checks:
    * output_dir must be non-empty (except if allow_empty=True)
    * _temporary subdir shouldn't be present

    >>> testls = lambda *a, **kw: paths
    >>> paths = ['/outputdir/part-000']
    >>> job_succeeded('/output_dir/', _ls=testls)
    True
    >>> paths = []
    >>> job_succeeded('/output_dir/', _ls=testls)
    False
    >>> paths = ['/output_dir/something', '/dev/output_dir/_temporary']
    >>> job_succeeded('/output_dir/', _ls=testls)
    False
    >>> paths = []
    >>> job_succeeded('/output_dir/', _ls=testls, allow_empty=True)
    True
    """
    paths = _ls(output_dir)
    return bool(paths or allow_empty) and all('_temporary' not in p for p in paths)

def dumbo_args(prog, **options):
    """Returns a list of args suitable to execute as indexer job

    >>> dumbo_args('path.to.job', libegg=['legg1', 'legg2'],
    ...    cachefile='/etc/passwd', ignoreMe=None, ignoremetoo=[])
    ['dumbo.cmd', 'start', 'path.to.job', '-libegg', 'legg1', '-libegg', 'legg2', '-cachefile', '/etc/passwd']
    """
    return ['dumbo.cmd', 'start', prog] + hdfs.hadoop_options(**options)


class DumboTask(PythonTask):
    """A Task to run dumbo scripts"""
    program = None
    output = None
    param = ()
    opts = ()

    _DUMBO_ATTRS = ('input', 'libjar', 'libegg', 'cachefile', 
                    'cachearchive', 'numreducetasks', 
                    'nummaptasks')

    def execute(self):
        if self.output:
            return self._execute_to_output()
        self.execargs = self._execargs()
        return PythonTask.execute(self)

    def _execute_to_output(self):
        # Skip task if output already exists
        if hdfs.path_exists(self.output):
            self.log('Output path already exists %s', self.output)
            return

        # Define an intermediate output dir
        wip = '%s_wip' % self.output.rstrip('/')
        if hdfs.dus(wip + '*'):
            self.log('Removing intermediate outputs found under %s*', wip)
            hdfs.rmr(wip + '*')
            sleep(3) # give hdfs a chance to remove dir before job recreate it

        # Compute dumbo args and execute dumbo program
        self.execargs = self._execargs(output=wip)
        PythonTask.execute(self)

        # Check dumbo program output and move output to final path
        assert job_succeeded(wip), 'Intermediate output is invalid, check %s' % wip
        self.log('wip dir output is valid, moving to %s', self.output)
        hdfs.mv(self.output, wip)

    def _execargs(self, **kwargs):
        kwargs.update((a, getattr(self, a, None)) for a in self._DUMBO_ATTRS)
        kwargs['param'] = ['%s=%s' % (k, v) for k, v in dict(self.param).items()]
        kwargs.update(self.opts)
        return dumbo_args(self.program, **kwargs)

