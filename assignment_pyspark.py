from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofweek, dayofmonth,to_date, year, month, count, avg, stddev

#Creating Spark session
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

#deriving cols, filtering type, aggregating type and magnitude, finally sorting based on DayOfWeek and writing result
df = df.withColumn('Date', to_date(col('Date'), 'MM/dd/yyyy')) \
        .withColumn('DayOfWeek', dayofweek(col('Date'))) \
        .withColumn('DayOfMonth', dayofmonth(col('Date'))) \
        .withColumn('Year', year(col('Date'))) \
        .withColumn('Month',month(col('Date')))

result = df.filter(col('Type') == 'Earthquake') \
            .groupBy('DayOfWeek') \
            .agg({"Type":"count","Magnitude":"avg"}) \
            .orderBy('DayOfWeek') \
            .write.csv("1.csv", header=True, mode="overwrite")

# inference : if avg(magnitude) increases for a particular day of week comparatively, then the number of earthquakes also high on that paritcular day of week


#2-What is the relation between Day of the month and Number of earthquakes that happened in a year?

#filtering type, aggregating type and magnitude on year ,finally sorting based on Year and writing result
result = df.filter(col('Type') == 'Earthquake').groupBy('Year').agg({"Type":"count","Magnitude":"avg"})
result.orderBy('Year').write.csv("2.csv", header=True, mode="overwrite")

#Inference: I could not find any inference, since the data fluctuates


#3-What does the average frequency of earthquakes in a month from the year 1965 to 2016 tell us?


#filtering year and type , grouping based on month and then counting the type and then taking avg and writing result
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

# extract it into separate lists
years = [row['Year'] for row in year_count_data]
counts = [row['count(Type)'] for row in year_count_data]

#finding avg, above avg and below avg
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

# Inference:after 1983 ,the earthquake occurs each year is > avg(earthquake) occured throughout all the year, 1991,1998(it shud be above-average, but its below average),1976(it shud be below-average, but its above average) so these 3 are exceptional


#5-How has the earthquake magnitude on average been varied over the years?

#converting Magnitude col to float type, filtering type, finding avg for magnitude and ordering the same and writing to csv
df.withColumn('Magnitude', col('Magnitude').cast('float')).filter(col('Type') == 'Earthquake').groupBy('Year').agg(avg('Magnitude')).orderBy('avg(Magnitude)').write.csv("5.csv", header=True, mode="overwrite")

#Result:
# +----+------------------+
# |Year|    avg(Magnitude)|
# +----+------------------+
# |null| 5.666666666666667|
# |1977| 5.784172628709166|
# |1973| 5.807614196980666|
# |1982| 5.816764687089359|
# |1978| 5.819999973683418|
# |1979| 5.823906692749557|
# |1976| 5.826174484269997|
# |1974|  5.82859152270035|
# |1989| 5.844608868403364|
# |1975|  5.84713214769625|
# |1988| 5.849060520498638|
# |1980| 5.849552211476795|
# |1984| 5.850792259935138|
# |2004| 5.850910662769645|
# |2005| 5.855347064377891|
# |1986|5.8574173430765955|
# |2000|5.8582278315456175|
# |2006| 5.859645644510825|
# |1997| 5.859868397838191|
# |1990| 5.861660283030444|
# |1981| 5.863418504452934|
# |2011| 5.864747166633606|
# |2002| 5.866216195596231|
# |2009| 5.867117968472567|
# |1998| 5.868814413080511|
# |1996| 5.872037016020881|
# |2012| 5.872808963261294|
# |1994| 5.877411047931717|
# |2001| 5.878329542904891|
# |1983| 5.878690719604492|
# |1992|  5.88154133101155|
# |2013| 5.881778722212785|
# |1991| 5.883177561180614|
# |2003| 5.885731946807547|
# |2007| 5.886019712215976|
# |2014| 5.886916646361351|
# |1993| 5.888817184202133|
# |1999| 5.890605364145185|
# |2016| 5.892835812273819|
# |1985|5.8956410049373265|
# |2010| 5.899105542887515|
# |1995| 5.904329350120143|
# |1987| 5.904498950591604|
# |2008|5.9088582504452685|
# |2015| 5.913071727538857|
# |1972| 5.943814418979527|
# |1971| 5.972538847997399|
# |1967| 6.003921531228459|
# |1969|6.0099378653935025|
# |1965|6.0141592771254455|
# |1970| 6.037209290404652|
# |1966| 6.042274661330194|
# |1968|  6.08184816813705|
# +----+------------------+
# Inference:
# 2010,2013,1971,2015,1965,1968 has higest variabilty comparing other values, it means the magnitude is unstable
# 1965-1970 avg magnitude varies by ~1, went > 6.x
# whereas other years avg magnitude varies by ~0.0x to 0.x and maintained below 6


#6-How does year impact the standard deviation of the earthquakes?

#converting Magnitude col to double type, finding std-deviation for magnitude and ordering the same
temp = df.withColumn("Magnitude", df["Magnitude"].cast("double")).groupBy("Year").agg(stddev("Magnitude").alias("MagnitudeStdDev")).orderBy('MagnitudeStdDev')

#filteing values of MagnitudeStdDev who has gt 0.45, and writing to csv
temp.filter(col('MagnitudeStdDev')>0.45).write.csv("6.csv", header=True, mode="overwrite")

# Result:
# +----+-------------------+
# |Year|    MagnitudeStdDev|
# +----+-------------------+
# |2010|0.45010976314177314|
# |2013|0.45637261052134354|
# |1971|0.46079804269148955|
# |2015| 0.4636753312589082|
# |1965|0.46964683759796527|
# |1968|0.46976560527556044|
# +----+-------------------+
#inference:
# 1982-low stddev - means magnitude was more stable in this year
# 1965-high stddev


#7-Does geographic location have anything to do with earthquakes?
temp = df.dropDuplicates().filter(col('Type') == 'Earthquake').groupBy('latitude', 'longitude').count().orderBy(col("count").desc())
temp.filter(col("count")>=2).write.csv("7.csv", header=True, mode="overwrite")

# Result:
# +--------+---------+-----+
# |latitude|longitude|count|
# +--------+---------+-----+
# |    51.5|   -174.8|    4|
# |  34.416|  -118.37|    3|
# |   38.64|   142.75|    2|
# +--------+---------+-----+
# Inference - Yes,coastal area or island have high frequency of earthquakes, all above lats and longs are coastal/island (manually searced maps)


#8-Where do earthquakes occur very frequently?

# filtering type, grouping lats and longs, counting the type and ordering in desc, and extracting first row
result = df.dropDuplicates().filter(col('Type') == 'Earthquake').groupBy('latitude', 'longitude').count().orderBy(col("count").desc()).first()
result_df = spark.createDataFrame([result], ["latitude", "longitude", "count"])
result_df.write.csv("8.csv", header=True, mode="overwrite")
# Result:
# +--------+---------+-----+
# |latitude|longitude|count|
# +--------+---------+-----+
# |    51.5|   -174.8|    4|
# +--------+---------+-----+

#9-What is the relation between Magnitude, Magnitude Type , Status and Root Mean Square of the earthquakes?

df.filter(col('Type') == 'Earthquake').groupBy('Magnitude Type').agg({'Magnitude': 'mean', 'Root Mean Square': 'mean'}).write.csv("9a.csv", header=True, mode="overwrite")
df.filter(col('Type') == 'Earthquake').groupBy('Status').agg({'Magnitude': 'mean', 'Root Mean Square': 'mean'}).write.csv("9b.csv", header=True, mode="overwrite")

# Inference
# MH type has highest avg(magnitude) has lowest avg(RMS)
# Automatic earthquakes have a higher average Magnitude when compared to Reviewed earthquakes
# Automatic has no RMS, whereas Reviewed has RMS

# Result:(9a)
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
#9b:
# +---------+-----------------+---------------------+
# |   Status|   avg(Magnitude)|avg(Root Mean Square)|
# +---------+-----------------+---------------------+
# |Automatic|6.005615746446431|                 NULL|
# | Reviewed|5.866894024805921|   1.0227839920884805|
# +---------+-----------------+---------------------+
