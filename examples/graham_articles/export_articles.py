import sys
import csv
import simplejson as json
from xml.dom.minidom import Document

def main():
    _format, input, output = sys.argv[1:4]

    f = open(input)
    articles = json.loads(f.read())
    fwrt = open(output, 'w')
    if _format == 'csv':
        wrt = csv.writer(fwrt)
        for url, subject in articles:
            wrt.writerow([url, subject])
    elif _format == 'xml':
        doc = Document()
        e_articles = doc.createElement("articles")
        doc.appendChild(e_articles)
        for url, subject in articles:
            e_article = doc.createElement("article")
            e_article_name = doc.createElement("name")
            e_article_url = doc.createElement("url")
            e_article_name.appendChild(doc.createTextNode(subject))
            e_article_url.appendChild(doc.createTextNode(url))
            e_article.appendChild(e_article_name)
            e_article.appendChild(e_article_url)
            e_articles.appendChild(e_article)
        fwrt.write(doc.toprettyxml(indent="  "))
    fwrt.close()

if __name__ == '__main__':
    main()
