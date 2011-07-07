from unittest import TestCase
from sworkflow.tasks import Task
from sworkflow.tasks.workflow import Workflow, walk, CancelWorkflow

class TaskTestCase(TestCase):

    def test_required_attributes(self):
        task = Task()
        self.assertEqual(task.deps, ())


class WalkTestCase(TestCase):

    def test_class_declaration(self):
        class T1(Task):
            taskname = 'T1'

        class T2(Task):
            taskname = 'T2'
            deps = [T1]

        class T3(Task):
            taskname = 'T3'
            deps = [T2]

        class T4(Task):
            taskname = 'T4'
            deps = [T3, T1]

        self.assertEqual(map(type, walk(T4)), [T1, T2, T3, T4])

    def test_instance_declaration(self):
        t1 = Task(taskname='T1')
        t2 = Task(taskname='T2', deps=[t1])
        t3 = Task(taskname='T3', deps=[t2])
        t4 = Task(taskname='T4', deps=[t3, t1])
        self.assertEqual(list(walk(t4)), [t1, t2, t3, t4])

    def test_mixed_declaration(self):
        t1 = Task(taskname='T1')

        class T(Task):
            taskname = 'T'
            deps = [t1]

        t2 = Task(taskname='T2', deps=[T])
        t3 = Task(taskname='T3', deps=[t1, t2])

        class S(Task):
            taskname = 'S'
            deps = [T, t1, t3, T]

        self.assertEqual([t.taskname for t in walk(S)],
                ['T1', 'T', 'T2', 'T3', 'S'])

    def test_cyclic_reference(self):
        t1 = Task(taskname='T1')
        t2 = Task(taskname='T2', deps=[t1])
        t1.deps = [t2]
        self.assertRaises(AssertionError, list, walk(t1))

    def test_invalid_task(self):
        t1 = Task()
        t2 = Task(deps=[t1, None])
        self.assertRaises(AssertionError, list, walk(t2))


class WorkflowTestCase(TestCase):

    def setUp(self):
        self.executed = executed = []
        class MockTask(Task):
            def execute(self):
                executed.append(self)
        self.MockTask = MockTask

    def test_simple_workflow(self):
        t1 = self.MockTask()
        st = self.MockTask(deps=[t1])
        wf = Workflow(starttask=st)
        self.assertEqual(wf.tasks, [(0, t1, False), (1, st, False)])
        wf.execute()
        self.assertEqual(self.executed, [t1, st])

    def test_exclude_tasks_passing_none(self):
        t1 = self.MockTask()
        st = self.MockTask(deps=[t1])
        wf = Workflow(taskname='w1', starttask=st, exclude_tasks=None)
        self.assertEqual(wf.tasks, [(0, t1, False), (1, st, False)])
        wf.execute()
        self.assertEqual(self.executed, [t1, st])

    def test_exclude_tasks_by_position(self):
        t1 = self.MockTask()
        st = self.MockTask(deps=[t1])
        wf = Workflow(taskname='w1', starttask=st, exclude_tasks=['1'])
        self.assertEqual(wf.tasks, [(0, t1, False), (1, st, True)])
        wf.execute()
        self.assertEqual(self.executed, [t1])

    def test_exclude_tasks_by_classname(self):
        class Skipme(self.MockTask):
            pass

        t1 = self.MockTask()
        st = Skipme(deps=[t1])

        wf = Workflow(starttask=st, exclude_tasks=['Skipme'])
        self.assertEqual(wf.tasks, [(0, t1, False), (1, st, True)])
        wf.execute()
        self.assertEqual(self.executed, [t1])

    def test_include_tasks_passing_none(self):
        t1 = self.MockTask()
        st = self.MockTask(deps=[t1])
        wf = Workflow(taskname='w1', starttask=st, include_tasks=None)
        self.assertEqual(wf.tasks, [(0, t1, False), (1, st, False)])
        wf.execute()
        self.assertEqual(self.executed, [t1, st])

    def test_include_tasks_by_position(self):
        t1 = self.MockTask()
        st = self.MockTask(deps=[t1])
        wf = Workflow(taskname='w1', starttask=st, include_tasks=['1'])
        self.assertEqual(wf.tasks, [(0, t1, True), (1, st, False)])
        wf.execute()
        self.assertEqual(self.executed, [st])

    def test_include_tasks_by_classname(self):
        class IncludeMe(self.MockTask):
            pass

        t1 = self.MockTask()
        st = IncludeMe(deps=[t1])

        wf = Workflow(starttask=st, include_tasks=['IncludeMe'])
        self.assertEqual(wf.tasks, [(0, t1, True), (1, st, False)])
        wf.execute()
        self.assertEqual(self.executed, [st])

    def test_workflow_used_as_task(self):
        t0 = self.MockTask()
        t1 = self.MockTask(deps=[t0])
        w1 = Workflow(starttask=t1)
        t2 = self.MockTask(deps=[w1])
        w2 = Workflow(starttask=t2)
        self.assertEqual(w2.tasks, [(0, w1, False), (1, t2, False)])
        w2.execute()
        self.assertEqual(self.executed, [t0, t1, t2])

    def test_propagate_task_failure_as_is(self):
        class TaskFailed(Exception):
            pass

        class FailTask(self.MockTask):
            def execute(self):
                raise TaskFailed

        wf = Workflow(starttask=FailTask())
        self.assertRaises(TaskFailed, wf.execute)

        # fail even across sub workflows
        wf = Workflow(starttask=Workflow(starttask=FailTask()))
        self.assertRaises(TaskFailed, wf.execute)

    def test_cancelworkflow(self):
        class FailTask(self.MockTask):
            def execute(self):
                raise CancelWorkflow

        wf = Workflow(starttask=FailTask())
        wf.execute()

        # doesn't fail even across sub workflows
        wf = Workflow(starttask=Workflow(starttask=FailTask()))
        wf.execute()

    def test_template_expansion(self):
        # settings and task attributes must be expanded on execute
        task = Task(foo='$foo-$xta', bar='$bar')
        class MyWorkflow(Workflow):
            starttask = task
            settings = dict(xta='xta$foo')

        # Instanciate workflow with custom params that will be merged with settings
        wf = MyWorkflow(params=dict(foo='cof'))
        # Add settings after workflow was instanciated
        wf.settings['bar'] = 'bar$foo'
        # check that tasks weren't expanded til executed
        self.assertEqual(task.foo, '$foo-$xta')
        self.assertEqual(task.bar, '$bar')
        # run workflow and check expanded task attributes
        wf.execute()
        self.assertEqual(wf.settings, {'foo': 'cof', 'bar': 'bar$foo', 'xta': 'xta$foo'})
        self.assertEqual(task.foo, 'cof-xtacof')
        self.assertEqual(task.bar, 'barcof')
