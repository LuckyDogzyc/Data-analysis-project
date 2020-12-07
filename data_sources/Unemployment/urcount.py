import sys
from csv import reader
from pyspark import SparkContext

sc = SparkContext()
data = sc.textFile(sys.argv[1], 1).mapPartitions(lambda x: reader(x))

# result = data.map(lambda x: x[0]).sortBy(lambda x: x[0])
output = data.map(lambda x: str(x[0]) + "-1," + str(x[1][:-1]) + "\r\n" + str(x[0]) + "-2," + str(x[2][:-1]) + "\r\n" +
                            str(x[0]) + "-3," + str(x[3][:-1]) + "\r\n" + str(x[0]) + "-4," + str(x[4][:-1]) + "\r\n" +
                            str(x[0]) + "-5," + str(x[5][:-1]) + "\r\n" + str(x[0]) + "-6," + str(x[6][:-1]) + "\r\n" +
                            str(x[0]) + "-7," + str(x[7][:-1]) + "\r\n" + str(x[0]) + "-8," + str(x[8][:-1]) + "\r\n" +
                            str(x[0]) + "-9," + str(x[9][:-1]) + "\r\n" + str(x[0]) + "-10," + str(x[10][:-1]) + "\r\n"
                            + str(x[0]) + "-11," + str(x[11][:-1]) + "\r\n" + str(x[0]) + "-12," + str(x[12][:-1]))
output = output.sortBy(lambda x: x[0])
output.saveAsTextFile("urcount.out")


'''
module load python/gnu/3.6.5
module load spark/2.4.0
rm -rf urcount.out
hfs -rm -R urcount.out
spark-submit --conf \
spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
urcount.py URCSV.csv
hfs -getmerge urcount.out urcount.out
hfs -rm -R urcount.out
head urcount.out
'''
