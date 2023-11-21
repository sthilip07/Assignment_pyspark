from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofweek, dayofmonth,to_date, year, month, count, avg, stddev

spark = SparkSession.builder \
    .appName("Assignment") \
    .config("spark.jars", "/Users/thilip/Downloads/jars/mysql-connector-j-8.1.0.jar") \
    .getOrCreate()   

df = spark.read.format("csv").load("database.csv",header=True) 

#writing df to mysql db table
df.write \
  .format("jdbc") \
  .option("driver","com.mysql.cj.jdbc.Driver") \
  .option("url", "jdbc:mysql://localhost:3306/tempdb") \
  .option("dbtable", "neic_earthquakes") \
  .option("user", "root") \
  .option("password", "") \
  .option("autoCommit","true") \
  .mode("overwrite") \
  .save()

#reading as df from mysql db table
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/tempdb") \
    .option("driver", "com.mysql.jdbc.Driver") \
    .option("dbtable", "neic_earthquakes") \
    .option("user", "root") \
    .option("password", "") \
    .load()


#1-How does the Day of a Week affect the number of earthquakes?


df = df.withColumn('Date', to_date(col('Date'), 'MM/dd/yyyy')).withColumn('DayOfWeek', dayofweek(col('Date'))).withColumn('DayOfMonth', dayofmonth(col('Date'))).withColumn('Year', year(col('Date'))).withColumn('Month',month(col('Date')))
result = df.filter(col('Type') == 'Earthquake').groupBy('DayOfWeek').agg({"Type":"count","Magnitude":"avg"})
result.orderBy('DayOfWeek').write.csv("1.csv", header=True, mode="overwrite")

# Result:
# if avg(magnitude) increases for a particular day of week comparatively, then the number of earthquakes also high on that paritcular day of week


#2-What is the relation between Day of the month and Number of earthquakes that happened in a year?

result = df.filter(col('Type') == 'Earthquake').groupBy('Year').agg({"Type":"count","Magnitude":"avg"})
result.orderBy('Year').write.csv("2.csv", header=True, mode="overwrite")

#Result: I could not find any inference, since the data fluctuates


#3-What does the average frequency of earthquakes in a month from the year 1965 to 2016 tell us?


#filtering year and type = earthquake, grouping based on month and then counting the type and then taking avg
avg_freq_eq = df.filter((col('Year') >= 1965) & (col('Year') <= 2016)) \
.filter(col('Type') == 'Earthquake') \
.groupBy(month('Date').alias('Month')) \
.agg(count('Type').alias('EarthquakeCount')) \
.agg(avg('EarthquakeCount').alias('AverageCountEarthquakes'))
avg_freq_eq.write.csv("3.csv", header=True, mode="overwrite")

# Result:
# +-----------------------+
# |AverageCountEarthquakes|
# +-----------------------+
# |                1935.75|
# +-----------------------+


#4-What is the relation between Year and Number of earthquakes that happened in that year?

df1 = df.filter(col('Type') == 'Earthquake').groupBy('Year').agg(count('Type'))

# converting df to list
year_count_data = df1.select('Year', 'count(Type)').collect()

# Extract it into separate lists
years = [row['Year'] for row in year_count_data]
counts = [row['count(Type)'] for row in year_count_data]

average_count = sum(counts) / len(counts)

above_average_years = [year for year, count in zip(years, counts) if count > average_count]
below_average_years = [year for year, count in zip(years, counts) if count < average_count]

with open('4.txt', 'w') as file:
    # Write the lines to the file
    file.write(f"The average number of earthquakes per year is {average_count:.2f}.\n")
    file.write(f"Years with above-average earthquake counts: {above_average_years}\n")
    file.write(f"Years with below-average earthquake counts: {below_average_years}\n")


#Result:
# The average number of earthquakes per year is 438.34.
# Years with above-average earthquake counts: [1990, 2003, 2007, 2015, 2006, 2013, 1988, 1997, 1994, 2014, 2004, 1989, 1996, 1985, 2012, 1987, 2009, 2016, 1995, 2001, 1992, 1983, 2005, 1984, 2000, 2010, 1986, 2011, 1976, 2008, 1999, 2002, 1993]
# Years with below-average earthquake counts: [1975, 1977, 1974, 1978, None, 1968, 1973, 1979, 1971, 1966, 1991, 1967, 1969, 1982, 1998, 1965, 1970, 1980, 1972, 1981]

# insights:after 1983 ,the earthquake occurs each year is > avg(earthquake) occured throughout all the year, 1991,1998(it shud be above-average, but its below average),1976(it shud be below-average, but its above average) so these 3 are exceptional


#5-How has the earthquake magnitude on average been varied over the years?

df.withColumn('Magnitude', col('Magnitude').cast('float')).filter(col('Type') == 'Earthquake').groupBy('Year').agg(avg('Magnitude')).orderBy('avg(Magnitude)').write.csv("5.csv", header=True, mode="overwrite")

# Result:
# 2010,2013,1971,2015,1965,1968 has higest variabilty comparing other values, it means the magnitude is unstable
# 1965-1970 avg magnitude varies by ~1, went > 6.x
# whereas other years avg magnitude varies by ~0.0x to 0.x and maintained below 6


#6-How does year impact the standard deviation of the earthquakes?


temp = df.withColumn("Magnitude", df["Magnitude"].cast("double")).groupBy("Year").agg(stddev("Magnitude").alias("MagnitudeStdDev")).orderBy('MagnitudeStdDev')
temp.filter(col('MagnitudeStdDev')>0.45).write.csv("6.csv", header=True, mode="overwrite")

# Result:
# 1982-low stddev - means magnitude was more stable in this year
# 1965-high stddev


#7-Does geographic location have anything to do with earthquakes?
temp = df.dropDuplicates().filter(col('Type') == 'Earthquake').groupBy('latitude', 'longitude').count().orderBy(col("count").desc())
temp.filter(col("count")>=2).write.csv("7.csv", header=True, mode="overwrite")

# Result - Yes,coastal area or island have high frequency of earthquakes


#8-Where do earthquakes occur very frequently?

result = df.dropDuplicates().filter(col('Type') == 'Earthquake').groupBy('latitude', 'longitude').count().orderBy(col("count").desc()).first()
result_df = spark.createDataFrame([result], ["latitude", "longitude", "count"])
result_df.write.csv("8.csv", header=True, mode="overwrite")
# Result:
# Row(latitude='51.5', longitude='-174.8', count=4)


#9-What is the relation between Magnitude, Magnitude Type , Status and Root Mean Square of the earthquakes?

df.filter(col('Type') == 'Earthquake').groupBy('Magnitude Type').agg({'Magnitude': 'mean', 'Root Mean Square': 'mean'}).write.csv("9a.csv", header=True, mode="overwrite")
df.filter(col('Type') == 'Earthquake').groupBy('Status').agg({'Magnitude': 'mean', 'Root Mean Square': 'mean'}).write.csv("9b.csv", header=True, mode="overwrite")

# MH type has highest avg(magnitude) has lowest avg(RMS)
# Automatic earthquakes have a higher average Magnitude when compared to Reviewed earthquakes
# Automatic has no RMS, whereas Reviewed has RMS


# +--------------+------------------+---------------------+
# |Magnitude Type|    avg(Magnitude)|avg(Root Mean Square)|
# +--------------+------------------+---------------------+
# |          NULL| 5.706666628519694|                 NULL|
# |           MWB| 5.907282324706373|   0.9844191825651049|
# |           MWC| 5.858176025643637|    1.012102870443265|
# |            MW| 5.933794334619597|   1.0882880151020742|
# |            MD|5.9666666984558105|  0.19833333045244217|
# |            MB| 5.682956632417873|   0.9890512681281349|
# |            MS| 5.994359559374046|    1.105131817002812|
# |           MWW| 6.008673713836054|   0.9620747950813565|
# |           MWR| 5.630769197757427|   0.9381818148222837|
# |            MH| 6.540000057220459|  0.11380000058561564|
# |            ML|  5.81467530015227|   0.4633789478409055|
# +--------------+------------------+---------------------+

# +---------+-----------------+---------------------+
# |   Status|   avg(Magnitude)|avg(Root Mean Square)|
# +---------+-----------------+---------------------+
# |Automatic|6.005615746446431|                 NULL|
# | Reviewed|5.866894024805921|   1.0227839920884805|
# +---------+-----------------+---------------------+