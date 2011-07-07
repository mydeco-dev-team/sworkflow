from sworkflow.ctl import cmdline, WorkflowControl
from workflow import GrahamWorkflow

workflows = {
    'process_articles' : GrahamWorkflow,
}

workflowcontrol = WorkflowControl(workflows)

if __name__ == '__main__':
    cmdline(workflowcontrol)
