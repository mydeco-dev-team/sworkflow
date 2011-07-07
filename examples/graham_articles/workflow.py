from sworkflow.tasks import PythonTask, Workflow, FsActionTask

class SetupDirs(FsActionTask):
    operation = 'mkdir'
    paths = ['$stage', '$output']

class DownloadArticles(PythonTask):
    execargs = ['download_articles', '$download_html']
    deps = [SetupDirs]

class ParseArticles(PythonTask):
    execargs = ['parse_articles', '$download_html', '$parsed']
    deps = [DownloadArticles]

class ExportArticlesCSV(PythonTask):
    execargs = [
        'export_articles', 'csv', '$parsed', '$stage/articles.csv'
    ]
    deps = [ParseArticles]

class ExportArticlesXML(PythonTask):
    execargs = [
        'export_articles', 'xml', '$parsed', '$stage/articles.xml'
    ]
    deps = [ParseArticles]

class CommitData(FsActionTask):
    operation = 'cp'
    paths = ['$stage/articles.csv', '$stage/articles.xml']
    dest = '$output'
    deps = [ExportArticlesXML, ExportArticlesCSV]


class GrahamWorkflow(Workflow):
    starttask = CommitData
    settings = {
        'stage': '/tmp/_stage',
        'download_html': '$stage/graham_articles.html',
        'parsed': '$stage/graham_articles.json',
        'output': '/tmp/graham_output',
    }
