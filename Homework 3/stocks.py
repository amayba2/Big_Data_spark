import os
import sys
import pandas as pd



os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python2.7'
os.environ['PYSPARK_SUBMIT_ARGS'] = ('--packages com.databricks:spark-csv_2.10:1.5.0 pyspark-shell')
os.environ['JAVA_TOOL_OPTIONS'] = "-Dhttps.protocols=TLSv1.2"

sys.path.append('/usr/lib/spark/python')
sys.path.append('/usr/lib/spark/python/lib/py4j-0.9-src.zip')

from pyspark import SparkContext, SparkConf, HiveContext
conf = SparkConf()
sc = SparkContext(conf = conf)
sqlContext = HiveContext(sc)


df = sqlContext.read.format('com.databricks.spark.csv') \
    .options(header='true', inferschema='true') \
    .load('tick_data.csv')

df.registerTempTable('stocks')
#2A1
sqlContext.sql("""SELECT distinct(SYM_ROOT) FROM stocks""").show()
#2A2
sqlContext.sql(""" SELECT DISTINCT(DATE) FROM stocks""").show()

print ("There is one date and four unique stocks")
print

#2B1
df1 = df.toPandas()
df1['Date2']=df1['DATE']
index =pd.DatetimeIndex( pd.to_datetime(df1["DATE"].apply(str) + " "+ df1['TIME_M']))
df1.set_index(index,inplace=True)

df1.drop("TIME_M",axis=1,inplace=True)
vol = df1.groupby([df1.index.hour,'SYM_ROOT'])['SIZE'].sum().reset_index(name ='SIZE_H')



#2B2
hour_first = df1.groupby([df1.index.hour,'SYM_ROOT']).first()['TRADE'].reset_index(name = 'First')
hour_last = df1.groupby([df1.index.hour,'SYM_ROOT']).last()['TRADE'].reset_index(name = 'Last')

final = vol
df2 = hour_first
df2['Last'] = hour_last['Last']
final['RETURN'] = (df2['Last']-df2['First'])/(df2['First'])

out = pd.DataFrame({'DATE':'2012-01-03','TIME_H':final['level_0'],'SYM_ROOT':final['SYM_ROOT'],'SIZE_H': final['SIZE_H'],'RETURN':final['RETURN']})

#If you want to see the table as a dataframe
#print out

rdd = sqlContext.createDataFrame(out)
out.to_csv('out.csv')

# rdd.write.format('com.databricks.spark.csv') \
#     .option('header', 'true') \
#     .save('out.csv')