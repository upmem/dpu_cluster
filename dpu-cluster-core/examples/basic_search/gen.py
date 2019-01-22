import sys
import requests
import random

if len(sys.argv) != 3:
    print 'usage: gen.py <number of bytes> <output>'
    exit(1)

word_site = "http://svnweb.freebsd.org/csrg/share/dict/words?view=co&content-type=text/plain"

response = requests.get(word_site)
WORDS = response.content.splitlines()

nr_entries = int(sys.argv[1]) / 32
output = open(sys.argv[2], "w")

for i in range(nr_entries):
    while True:
        first = WORDS[random.randint(0, len(WORDS) - 1)]
        if len(first) < 16:
            break
    second = WORDS[random.randint(0, len(WORDS) - 1)]
    entry = (first + "," + second)[:31]
    for _ in range(len(entry), 32):
        entry = entry + "\0"

    output.write(entry)

output.close()
