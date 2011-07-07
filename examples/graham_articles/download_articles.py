import sys
import urllib

def main():
    output = sys.argv[1]
    url = "http://www.paulgraham.com/articles.html"
    html = urllib.urlopen(url).read()
    f = open(output, 'w')
    f.write(html)
    f.close()

if __name__ == '__main__':
    main()
