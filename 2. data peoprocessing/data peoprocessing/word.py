import sys
import re
import string
import nltk

IN = open(sys.argv[1], "r")
OUT = open(sys.argv[1]+".processed", "w")
OUT.write("time\tuserID\ttweetWords\thashtags\tPOI\tGEO\n")

TARGETS = [2,3]
DELIMITER = "\"\"\""
table = str.maketrans('', '', string.punctuation)
stop_words = set(nltk.corpus.stopwords.words('english'))
ps = nltk.stem.PorterStemmer()

for line in IN:
	info = line.rstrip().split(DELIMITER)
	if len(info) != 6:
		continue
	for i in TARGETS:
		text = info[i]
		trimmedLine = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|(\\[u][A-Za-z0-9]{4})"," ",text).split())
		tokens = [w.lower().translate(table) for w in nltk.tokenize.word_tokenize(trimmedLine)]
		words = [ps.stem(word) for word in tokens if word.isalpha() and not word in stop_words]
		info[i]=",".join(words)
	printLine = "\t".join(info)
	OUT.write(printLine+"\n")
OUT.close()