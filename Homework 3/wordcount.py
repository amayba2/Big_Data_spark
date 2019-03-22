import os
import sys
import string




os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python2.7'

sys.path.append('/usr/lib/spark/python')
sys.path.append('/usr/lib/spark/python/lib/py4j-0.9-src.zip')

from pyspark import SparkContext, SparkConf

conf = SparkConf()
sc = SparkContext(conf = conf)

text =sc.textFile('pg100.txt')



def clean(x):

    coded = x.encode('utf-8')
    lower = coded.lower()

    cleaned = lower.translate(None, string.punctuation)
    cleaned = cleaned.translate(None,string.digits)
    return cleaned



words = text.flatMap(lambda x: clean(x).split(' '))
words = words.filter(lambda x: len(x)> 1)
count = words.map(lambda x: (x[1], 1)).reduceByKey(lambda x,y: x+y).map(lambda x:  (x[1], x[0])).sortByKey(False).map(lambda x: (x[1],  x[0]))

out =count.collect()

with open('out.txt', 'w') as f:
    for value in out:
        f.write(str(value)+'\n')



