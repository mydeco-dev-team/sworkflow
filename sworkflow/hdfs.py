"""
Helper functions for working with Hadoop HDFS. Most function are simple
wrappers around the "hadoop fs" command.
"""

import sys
from posixpath import join
from subprocess import Popen, PIPE, call, check_call, CalledProcessError
from tempfile import TemporaryFile


## FsShell bindings

def ls(*paths, **options):
    """List the contents that match the specified file pattern

    Default output:
    >> hdfs.ls('/dev/example/path/*.txt')
    ['/dev/example/path/file.txt']

    Extended output:
    >> hdfs.ls('/dev/example/path/*.txt', extended=True)
    [{'date': '2010-06-16',
        'group': 'supergroup',
        'path': '/dev/example/path/file.txt',
        'perms': '-rw-r--r--',
        'replication': '3',
        'size': '7485',
        'time': '17:28',
        'user': 'root'}]

    """
    cols = ('perms', 'replication', 'user', 'group', 'size', \
            'date', 'time', 'path')
    fscmd = '-lsr' if options.get('recursive') else '-ls'
    extended = _hadoopfs_columns(cols, fscmd, *paths)
    return list(extended) if options.get('extended') \
            else [e['path'] for e in extended]

def lsr(*paths, **options):
    """Recursive ls for HDFS"""

    options['recursive'] = True
    return ls(*paths, **options)

def mv(dst, *src):
    """Move files that match the specified file pattern <src> to a destination <dst>.

    When moving multiple files, the destination must be a directory.
    """
    check_call(('hadoop', 'fs', '-mv') + src + (dst,))

def cp(dst, *src):
    """Copy files from source to destination

    This command allows multiple sources as well in which case the destination must be a directory
    """
    check_call(('hadoop', 'fs', '-cp') + src + (dst,))

def rm(*paths):
    """Delete files specified as args

    Only deletes non empty directory and files. Refer to rmr for recursive deletes
    """
    check_call(('hadoop', 'fs', '-rm') + paths)

def rmr(*paths):
    """Recursive version of delete"""
    check_call(('hadoop', 'fs', '-rmr') + paths)

def put(dst, *src, **options):
    """Copy files from the local file system into hdfs"""
    if options.get('overwrite') and path_exists(dst):
        rmr(dst)
    if 'stdin' in options:
        check_call(('hadoop', 'fs', '-put', '-', dst), stdin=options['stdin'])
    else:
        check_call(('hadoop', 'fs', '-put') + src + (dst,))

def get(dst, *src):
    """Copy files from hdfs into the local file system"""
    check_call(('hadoop', 'fs', '-get') + src + (dst,))

def cat(*paths):
    """Return a file-like object with the output of the given paths"""
    return Popen(('hadoop', 'fs', '-cat') + paths, stdout=PIPE).stdout

def mkdir(*paths, **options):
    """Create a directory in the specified location"""
    errbuf = TemporaryFile()
    try:
        check_call(('hadoop', 'fs', '-mkdir') + paths, stderr=errbuf)
    except CalledProcessError, ex:
        if options.get('fail_if_exists') or ex.returncode != 255:
            errbuf.seek(0)
            print >> sys.stderr, errbuf.read()
            raise

def touchz(*paths):
    """Write a timestamp in yyyy-MM-dd HH:mm:ss format in a file at <path>.

    An error is returned if the file exists with non-zero length
    """
    check_call(('hadoop', 'fs', '-touchz') + paths)

def du(*paths):
    """Show the amount of space, in bytes, used by the files
    that match the specified file pattern.

    Equivalent to the unix command "du -sb <path>/*" in case of
    a directory, and to "du -b <path>" in case of a file.
    The output is in the form name(full path) size (in bytes)
    """
    cols = ('usage', 'path')
    return _hadoopfs_columns(cols, '-du', *paths)

def dus(*paths):
    """Show the amount of space, in bytes, used by the files
    that match the specified file pattern.

    Equivalent to the unix command "du -sb".
    The output is in the form name(full path) size (in bytes)
    """
    cols = ('path', 'usage')
    return _hadoopfs_columns(cols, '-dus', *paths)

def distcp(dst, *src, **options):
    """Copy file or directories recursively"""
    options = tuple(hadoop_options(**options))
    check_call(('hadoop', 'distcp') + options + src + (dst,))

def path_exists(path):
    """
    Returns True if the path exist on HDFS, else False.
    """
    cmd = ['hadoop', 'fs', '-test', '-e', path]
    retcode = call(cmd)
    if retcode > 1:
        raise CalledProcessError(retcode, cmd)
    return retcode == 0

def hadoop_options(**options):
    """Returns a list of single dashed arguments compatible with hadoop command line

    >>> hadoop_options(libegg=['legg1', 'legg2'], 
    ...    cachefile='/etc/passwd', ignoreMe=None, ignoremetoo=[])
    ['-libegg', 'legg1', '-libegg', 'legg2', '-cachefile', '/etc/passwd']

    >>> hadoop_options(update=True, delete=False, filelimit='1g')
    ['-update', '-filelimit', '1g']

    """
    args = []
    for k, values in options.iteritems():
        if values is True:
            args.append('-'+k)
        elif values is not None and values is not False:
            values = values if isinstance(values, list) else [values]
            args.extend(p for v in values for p in ['-'+k, v])
    return args

## helpers
def _hadoopfs_columns(cols, *args):
    stdout = Popen(('hadoop', 'fs') + args, stdout=PIPE).communicate()[0]
    rows = (line.strip().split() for line in stdout.splitlines())
    return [dict(zip(cols, row)) for row in rows if len(row) == len(cols)]


class HDFSOutputFile(object):
    """
    Helper to create a file on HDFS and write to it
    """
    def __init__(self, hdfspath):
        self.hdfspath = hdfspath
        self.buf = TemporaryFile()
        self.write = self.buf.write
        self.writelines = self.buf.writelines

    def close(self):
        self.buf.seek(0)
        if path_exists(self.hdfspath):
            # FIXME: removing and writing the new file should be made atomically
            rmr(self.hdfspath)
        check_call(['hadoop', 'fs', '-put', '-', self.hdfspath], stdin=self.buf)
        self.buf.close()

class HDFSInputFile(object):
    """
    Helper to read from a HDFS file
    """
    def __init__(self, hdfspath):
        self.hdfspath = hdfspath
        self.buf = TemporaryFile()
        check_call(['hadoop', 'fs', '-cat', self.hdfspath], stdout=self.buf)
        self.buf.seek(0)
        self.read = self.buf.read
        self.readlines = self.buf.readlines
        self.close = self.buf.close


class HDFSWriter(object):
    """
    Helper to write a file on HDFS but using part-XXXXX format.
    """
    def __init__(self, path, partsize=100000):
        assert path not in ('/', ''), "Cannot create HDFSWriter on %r" % path
        self.path = path
        if path_exists(path):
            rmr(path)
        self.partsize = partsize
        self.part = 0
        self.count = 0
        self.hadoop = None

    def write(self, line):
        if self.count % self.partsize == 0:
            if self.hadoop:
                self.hadoop.communicate()
            path = join(self.path, "part-%05d" % self.part)
            self.hadoop = Popen(['hadoop', 'fs', '-put', '-', path], \
                stdin=PIPE, close_fds=True)
            self.part += 1
        self.hadoop.stdin.write(line)
        self.count += 1

    def complete(self):
        if self.hadoop and self.hadoop.poll() is None:
            self.hadoop.communicate()

