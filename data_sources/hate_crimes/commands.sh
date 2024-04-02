#!/bin/bash

module load python/gnu/3.6.5
module load spark/2.4.0

output_file="hate_crimes_daily_out.csv"

rm -rf $output_file
/usr/bin/hadoop fs -rm -r $output_file

spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python /home/zd790/project/hate_crimes/hate_crimes_daily.py /user/zd790/NYPD_Hate_Crimes.csv

/usr/bin/hadoop fs -getmerge $output_file $output_file

/usr/bin/hadoop fs -rm -r $output_file