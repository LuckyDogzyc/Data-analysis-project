import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("hate_crimes") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

hate_crimes = spark.read.format('csv').options(header='true',
                                               inferschema='false').load(sys.argv[1])

hate_crimes.createOrReplaceTempView("hate_crimes")

spark.sql("""
SELECT `Record Create Date`,
	COUNT(CASE WHEN `Bias Motive Description`='ANTI-WHITE' THEN 1 END) AS anti_white,
    COUNT(CASE WHEN `Bias Motive Description`='ANTI-ASIAN' THEN 1 END) AS anti_asian,
    COUNT(CASE WHEN `Bias Motive Description`='ANTI-BLACK' THEN 1 END) AS anti_black,
    COUNT(CASE WHEN `Bias Motive Description`='ANTI-JEWISH' THEN 1 END) AS anti_jewish,
    COUNT(CASE WHEN `Bias Motive Description`='ANTI-HISPANIC' THEN 1 END) AS anti_hispanic
FROM hate_crimes
GROUP BY `Record Create Date`
ORDER BY `Record Create Date`
          """).write.csv("hate_crimes_daily_out.csv", nullValue=None, quote="")
