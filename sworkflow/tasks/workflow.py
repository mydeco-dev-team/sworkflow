"""
A simple workflow engine
"""
import logging
from collections import defaultdict
from datetime import datetime
from string import Template

from .task import Task


class ExitWorkflow(Exception):
    """Exit the execution of a workflow earlier"""

    # Special exit status to be used by spawned programs to stop workflow
    # not in sysexits.h, just to say that we require the process to stop
    EXIT_STOPPED = 90
    # EX_SOFTWARE according to /usr/include/sysexits.h
    EXIT_FAILED = 70
    # EX_TEMPFAIL according to /usr/include/sysexits.h
    EXIT_CANCELLED = 75

    status_messages = {
        EXIT_STOPPED: "EXIT: STOPPED",
        EXIT_FAILED: "EXIT: FAILED",
        EXIT_CANCELLED: "EXIT: CANCELLED"
    }

    def __init__(self, task, status, message=None):
        self.task = task
        self.status = status
        Exception.__init__(self, message)

    @classmethod
    def is_status(cls, status):
        return status in cls.status_messages

    def get_exit_message(self):
        return self.status_messages.get(self.status, "")


class Workflow(Task):
    """Control the execution of a workflow object"""

    starttask = None
    include_tasks = ()
    exclude_tasks = ()
    settings = ()

    def __init__(self, **kwargs):
        params = kwargs.pop('params', {})
        Task.__init__(self, **kwargs)
        self.settings = dict(self.settings, **params)
        self.tasks = list(self._tasks())

    def _tasks(self):
        tasks = walk(self.starttask)
        exclude = self.exclude_tasks or ()
        include = self.include_tasks or ()
        for i, task in enumerate(tasks):
            taskid = task.__class__.__name__
            if include:
                skipped = (taskid not in include) and (str(i) not in include)
            elif exclude:
                skipped = (taskid in exclude) or (str(i) in exclude)
            else:
                skipped = False
            yield i, task, skipped

    def _execute(self):
        esettings = _tsettings(self.settings)
        for i, task, skipped in self.tasks:
            if skipped:
                self.log('Task skipped: %i-%s', i, task)
                continue

            _texpand(task, esettings)
            starttime = datetime.now()
            self.log('Task started: %i-%s', i, task)
            try:
                task.execute()
            except Exception:
                self.log('Task failed: %i-%s in %s', i, task, \
                        datetime.now() - starttime, level=logging.ERROR)
                raise
            else:
                self.log('Task succeed: %i-%s in %s', i, task, \
                        datetime.now() - starttime)

    def execute(self):
        starttime = datetime.now()
        self.log('Workflow started')
        try:
            self._execute()
        except ExitWorkflow, exc:
            tmsg = "Task %s stopped the workflow with exit status '%s' in %s"
            msg = tmsg % (exc.task, exc.get_exit_message(),
                    datetime.now() - starttime)
            if exc.status == ExitWorkflow.EXIT_STOPPED:
                self.log('Workflow stopped: %s' % msg)
            elif exc.status == ExitWorkflow.EXIT_CANCELLED:
                self.log('Workflow cancelled: %s' % msg)
            elif exc.status == ExitWorkflow.EXIT_FAILED:
                self.log('Workflow failed: %s' % msg)
                raise
            else:
                raise
        except Exception:
            self.log('Workflow failed in %s', \
                    datetime.now() - starttime, level=logging.ERROR)
            raise
        else:
            self.log('Workflow succeed in %s', datetime.now() - starttime)


def walk(starttask):
    """Walk starttask and build ordered list of subtasks to execute

    >>> t1 = Task(taskname='T1')
    >>> t2 = Task(taskname='T2', deps=[t1])
    >>> t3 = Task(taskname='T3', deps=[t2])
    >>> t4 = Task(taskname='T4', deps=[t3, t1])
    >>> [t.taskname for t in walk(t4)]
    ['T1', 'T2', 'T3', 'T4']

    """
    clsmap = {} 
    def _n(task):
        if type(task) is type:
            # task referenced by its class is instanciated once
            task = clsmap.get(task) or clsmap.setdefault(task, task())
        assert isinstance(task, Task), \
                'Require a Task instance, got %s' % type(task)
        return task

    def _dfs(task, ttl=500):
        assert ttl, 'DFS reached depth limit, check for cyclic dependencies'
        yield task
        for dep in map(_n, task.deps):
            yield dep
            for subdep in _dfs(dep, ttl-1):
                yield subdep

    seen = set()
    for t in reversed(list(_dfs(_n(starttask)))):
        if t not in seen:
            seen.add(t)
            yield t

def find_redundant_deps(starttask):
    """Returns a list of of tuples of the form (task, dep, seen_in) where:
        * (task, dep) is the dependency to remove
        * seen_in is a list saying which are the linked task where the dependency ocurrs

    Doctest:
     >>> class Task0(Task):
     ...    pass
     >>> class Task1(Task):
     ...    deps = [Task0]
     >>> class Task2(Task):
     ...    deps = [Task0, Task1]
     >>> class Task3(Task):
     ...    deps = [Task0, Task2]
     >>> list(find_redundant_deps(Task3))
     [(('Task2', 'Task0'), ['Task1']), (('Task3', 'Task0'), ['Task1', 'Task2'])]
    """
    seen = defaultdict(set)
    for task in walk(starttask):
        tid = taskid(task)
        taskpath = [taskid(s) for s in walk(task)]
        for dep in task.deps:
            depid = taskid(dep)
            seenin = [t for t in taskpath if t in seen[depid]]
            if seenin:
                yield (tid, depid), seenin
            seen[depid].add(tid)

def taskid(task):
    """Returns the task id"""
    return task.__name__ if type(task) is type else task.__class__.__name__

def _texpand(task, settings):
    """Expand templates found in task attributes

    >>> t = Task(output='$path/$leaf', leaf='useless', path=['$prefix/path'])
    >>> settings = dict(prefix='/tmp', path='/tmp/path/tmp')
    >>> _texpand(t, settings)
    >>> t.output
    '/tmp/path/tmp/$leaf'

    >>> t.path
    ['/tmp/path']
    >>> settings['leaf'] = 'dir'
    >>> _texpand(t, settings)
    >>> t.output
    '/tmp/path/tmp/dir'
    """
    for attr in dir(task):
        if attr.startswith("_"):
            continue
        v = getattr(task, attr)
        if not callable(v):
            nv = _titem(v, settings)
            try:
                setattr(task, attr, nv)
            except AttributeError:
                # it may happen that the attribute is a property
                pass

def _tsettings(settings):
    """Returns expanded settings

    Expand same template used twice in same value
    >>> _tsettings(dict(prefix='/tmp', path='$prefix/path$prefix'))
    {'path': '/tmp/path/tmp', 'prefix': '/tmp'}

    Looping expansion must fail
    >>> try:
    ...     _tsettings(dict(loopvar='$loopvar'))
    ... except AssertionError, ex:
    ...     if 'Recursive value found' not in str(ex):
    ...         raise
    ... else:
    ...     raise AssertionError('loopvar expansion ignored')
    >>> try:
    ...     _tsettings(dict(var1='$var2', var2='$var1'))
    ... except AssertionError, ex:
    ...     if 'Recursive value found' not in str(ex):
    ...         raise
    ... else:
    ...     raise AssertionError('loopvar expansion ignored')

    """
    tvars = dict(settings)
    modified = True
    while modified:
        modified = False
        for k, v in tvars.items():
            if isinstance(v, basestring) and '$' in v:
                assert k not in v or v == _tsub(v, {k: ''}), \
                    "Recursive value found during expansion: %r" % v
                nv = _tsub(v, tvars)
                if nv != v:
                    tvars[k] = nv
                    modified = True
    return tvars

def _titem(v, tvars):
    """Replace templates

    >>> tvars = dict(a=1, b=2)
    >>> _titem('$a/$b', tvars)
    '1/2'
    >>> _titem(['$a', '$b'], tvars)
    ['1', '2']
    >>> _titem(dict(r='$a', c=2), tvars)
    {'c': 2, 'r': '1'}
    >>> _titem(['$a', 1, dict(r='$a', c=2), '$b'], tvars)
    ['1', 1, {'c': 2, 'r': '1'}, '2']
    >>> _titem(('$a', 1, '$b'), tvars)
    ('1', 1, '2')
    >>> deps = [object(), object()]
    >>> _titem(deps, tvars) == deps
    True

    """
    if isinstance(v, list):
        return [_titem(v, tvars) for v in v]
    elif isinstance(v, tuple):
        return tuple(_titem(v, tvars) for v in v)
    elif isinstance(v, dict):
        return dict((k, _titem(v, tvars)) for k, v in v.iteritems())
    elif isinstance(v, basestring) and '$' in v:
        return _tsub(v, tvars)
    else:
        return v

def _tsub(tmpl, *args, **kwargs):
    return Template(tmpl).safe_substitute(*args, **kwargs)

