import re
import sys
import simplejson as json

URL_PARSE = re.compile('paulgraham_2158_7512.+?<a href="([^\"]+?)".*?>([^<]+?)</a>')

def main():
    input, output = sys.argv[1:3]
    f = open(input)
    articles = f.read()
    f.close()
    larticles = URL_PARSE.findall(articles)
    f = open(output, 'w')
    f.write(json.dumps(larticles))
    f.close()

if __name__ == '__main__':
    main()

