import os
import shutil
from sworkflow.tasks.task import Task

class FsActionTask(Task):
    """
    FS action task apply hdfs operation when run, this are the operations 
    allowed: move/copy/mkdir/rm/rmr/path_exists
    """

    operation = None
    paths = ()
    dest = None
    options = None

    def execute(self):
        cmd = self.operation
        options = self.options or {}
        assert self.paths, 'no paths were set: %s' % self.paths
        if cmd == 'mkdir':
            for path in self.paths:
                os.mkdir(path)
        elif cmd == 'cp':
            for path in self.paths:
                if os.path.isdir(path):
                    shutil.copytree(path, self.dest)
                else:
                    shutil.copy(path, self.dest)
        elif cmd == 'mv':
            for path in self.paths:
                shutil.move(self.dest, *self.paths)
        elif cmd == 'rm':
            for path in self.paths:
                os.remove(path)
        elif cmd == 'rmr':
            for path in self.paths:
                os.rmdir(path)
        elif cmd == 'path_exists':
            for path in self.paths:
                assert os.path.exists(path), 'path does not exists %s' % path
        else:
            raise RuntimeError("Unknown operation: %s" % cmd)
