from sworkflow import hdfs
from sworkflow.tasks.fstask import FsActionTask

class HDFSActionTask(FsActionTask):
    """
    HDFS action task apply hdfs actions when run over the filesystem, this are 
    the operations allowed:
        - move/copy
        - mkdir/rm/rmr
        - test path existence
        - distcp

    Examples:
     * mkdir/rm/rmr:

       HDFSOperationTask(operation='mkdir', paths=['/path/to/dir', 
          '/path/to/dir2'])

     * mv/cp/put:

       HDFSOperationTask(operation='mv', paths=['/path1', '/path2'], 
          dest='/dest')

       another option:

       class SpecialMove(HDFSOperationTask):
           operation = 'mv'
           paths = ["path1", "path2"]
           dest = "dest/path/"

     * path_exists:

       HDFSOperationTask(operation='path_exists', paths=['/path/to/check'])

       another option:

       class ExistsLogData(HDFSOperationTask):
           operation = "path_exists"
           paths = ["/cqr/log/data"]
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
            hdfs.mkdir(*self.paths)
        elif cmd == 'put':
            hdfs.put(self.dest, *self.paths)
        elif cmd == 'cp':
            hdfs.cp(self.dest, *self.paths)
        elif cmd == 'mv':
            hdfs.mv(self.dest, *self.paths)
        elif cmd == 'rm':
            hdfs.rm(*self.paths)
        elif cmd == 'rmr':
            hdfs.rmr(*self.paths)
        elif cmd == 'path_exists':
            for path in self.paths:
                assert hdfs.path_exists(path), 'path does not exists %s' % path
        elif cmd == 'distcp':
            hdfs.distcp(self.dest, *self.paths, **options)
        else:
            raise RuntimeError("Unknown operation: %s" % cmd)
