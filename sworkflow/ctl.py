from optparse import OptionParser
from sworkflow.tasks.workflow import walk, taskid, find_redundant_deps


class WorkflowControl(object):
    """helper class for workflows"""

    def __init__(self, workflows):
        self.workflows = workflows

    def list(self):
        return self.workflows.keys()

    def create(self, name, **wfkwargs):
        try:
            cls = self.workflows[name]
        except KeyError:
            raise ValueError("Workflow doesn't exist: %s" % name)
        return cls(**wfkwargs)


def _parser():
    usage = "%prog [options] [run|list|list-tasks|draw] [workflow_name]"
    parser = OptionParser(usage=usage, description=__doc__)
    parser.add_option('--param', '-p', action='append', metavar='NAME=VALUE',
            help='Additional settings merged with workflow settings')
    parser.add_option('--include-task', '-t', metavar='TASKID', action='append',
            help='Run the given task id and skip others')
    parser.add_option('--exclude-task', '-s', metavar='TASKID', action='append',
            help='Skip the given task id')
    parser.add_option('-o', '--output', metavar="PATH",
            help='Path when using a command that output a file. eg. draw')
    parser.add_option('-R', '--ignore-redundant-deps', action='store_true',
            help='Remove redundant dependencies from workflow tree representation')
    return parser

def draw_workflow(workflow, workflow_name, filename=None, remove_dependencies=True):
    try:
        import pygraphviz as pgv
    except ImportError:
        print "You need PyGraphviz to run this command."
        return

    starttask = workflow.starttask
    redundant_deps = set(edge for edge, _  in find_redundant_deps(starttask))

    graph = pgv.AGraph(strict=False, directed=True)
    graph.graph_attr['label'] = "Workflow name: %s" % taskid(workflow)
    for task in walk(starttask):
        for t in task.deps:
            a = taskid(task)
            b = taskid(t)
            color = ''
            if (a, b) in redundant_deps:
                if remove_dependencies:
                    continue
                color = 'red'
            graph.add_edge(b, a, color=color)

    graph.layout(prog='dot')
    filename = filename or "%s%s.png" % (workflow_name, '-nodeps' if remove_dependencies else '')
    graph.draw(filename)
    print "Workflow graphical representation drawn : %s." % filename
    if remove_dependencies:
        print "Unnecesary dependencies were removed from the graph to make it"
        print "clear. Use 'lint' command if you want to see them"


def cmdline(controller):
    """Provide a command line interface to sworkflow"""

    parser = _parser()
    opts, args = parser.parse_args()
    if not args:
        parser.error("please specify a command, or -h for help")

    params = dict(p.strip().split("=") for p in opts.param or ())

    cmd = args[0]
    workflow = None
    if cmd in ('run', 'list-tasks', 'draw', 'lint', 'list-settings',): # need workflow name
        try:
            name = args[1]
        except IndexError:
            parser.error("'%s' command needs the workflow name" % args[0])

        workflow = controller.create(name, params=params,
                exclude_tasks=opts.exclude_task, include_tasks=opts.include_task)

    if cmd == 'run':
        workflow.execute()
    elif cmd == 'list':
        for wf in controller.list():
            print wf
    elif cmd == 'list-tasks':
        print " %3s | %-38s | %-9s | %s" % ('#', 'taskid', 'skipped?', 'name')
        print "-"*80
        for i, task, skipped in workflow.tasks:
            task_id = taskid(task)
            print " %3d | %-38s | %-9s | %s" % (i, task_id, skipped, task)
        print "-"*80
    elif cmd == 'lint':
        draw_workflow(workflow, name, filename=opts.output, remove_dependencies=False)
    elif cmd == 'draw':
        draw_workflow(workflow, name, filename=opts.output)
    elif cmd == 'list-settings':
        print "| %s | %s |" % ("setting name".ljust(28), "default".ljust(45))
        print "-"*80
        settings = workflow.settings or {}
        for name, default in settings.items():
            print "| %s | %s |" % (name.ljust(28), default.ljust(45),)
        print "-"*80
    else:
        parser.error("'%s' is not a valid command" % cmd)
