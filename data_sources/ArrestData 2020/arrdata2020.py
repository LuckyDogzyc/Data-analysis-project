from csv import reader
from pyspark import SparkContext
import sys
sc = SparkContext()

rdd = sc.textFile(sys.argv[1],1)
#rdd = sc.textFile("nydata2020.csv")
rdd = rdd.mapPartitions(lambda x: reader(x))
rdd = rdd.filter(lambda x: "ARREST_KEY" not in x)
#borough counts
boroughCounts = rdd.map(lambda x:(x[1]+","+x[8],1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[0])
boroughCounts.map(lambda x: x[0]+","+str(x[1])).saveAsTextFile("allout/boroughCounts.out")
#total counts
totalCounts = rdd.map(lambda x:(x[1],1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[0])
totalCounts.map(lambda x:x[0]+","+str(x[1])).saveAsTextFile("allout/totalCounts.out")

#Category counts in borough
category = {"a12","a3","l1","l2","l3","dd","dw","r","sc1","sc2","bg","cm","ct","vt","m"}
selectCategory = rdd.filter(lambda x:x[5] in category)
categoryCounts = selectCategory.map(lambda x:(x[1]+","+x[5]+","+x[8],1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[0])
categoryCounts.map(lambda x:x[0]+","+str(x[1])).saveAsTextFile("allout/categoryCountsBorough.out")
#Category counts in total
totalCategoryCounts = selectCategory.map(lambda x:(x[1]+","+x[5],1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[0])
totalCategoryCounts.map(lambda x:x[0]+","+str(x[1])).saveAsTextFile("allout/totalCategoryCounts.out")
#sex counts in each borough
sexCountsBorough = selectCategory.map(lambda x:(x[1]+","+x[12]+","+x[8],1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[0])
sexCountsBorough.map(lambda x:x[0]+","+str(x[1])).saveAsTextFile("allout/sexCountsBorough.out")
#total sexCounts
totalSexCounts = selectCategory.map(lambda x:(x[1]+","+x[12],1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[0])
totalSexCounts.map(lambda x:x[0]+","+str(x[1])).saveAsTextFile("allout/totalSexCounts.out")
#Age group in each borough
ageCountsBorough = selectCategory.map(lambda x:(x[1]+","+x[11]+","+x[8],1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[0])
ageCountsBorough.map(lambda x:x[0]+","+str(x[1])).saveAsTextFile("allout/ageCountsBorough.out")
#Age group in total
ageCountsBorough = selectCategory.map(lambda x:(x[1]+","+x[11],1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[0])
ageCountsBorough.map(lambda x:x[0]+","+str(x[1])).saveAsTextFile("allout/totalAgeCounts.out")
#percentage difference
