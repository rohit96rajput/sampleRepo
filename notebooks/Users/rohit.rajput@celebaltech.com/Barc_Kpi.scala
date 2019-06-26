// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.Row

// COMMAND ----------

val data=spark.read.format("csv").option("header","true").load("/FileStore/tables/Barc_Linear.csv")
val allData=data.withColumn("Date",date_format(to_date(col("Date"), "dd/MM/yyyy"), "yyyy-MM-dd"))

// COMMAND ----------

display(allData.select($"Channel").distinct)

// COMMAND ----------

display(allData.filter($"Programme Genre"==="GAMESHOW/QUIZ"))

// COMMAND ----------

display(allData.groupBy("Programme Genre").agg(countDistinct("Advertiser").alias ("advertiserCount"),sum("Length [sec]")).orderBy($"advertiserCount".desc))

// COMMAND ----------

val GRP=allData.filter($"Programme Genre"==="DRAMA/SOAP").groupBy("Channel").agg(sum("rat%").alias("grpTotal"),countDistinct("Advertiser").alias ("advertiserCount")).orderBy($"advertiserCount".desc).withColumn("adGRP",($"grpTotal"*1800/10)).withColumn("adGRPAdvertiser",$"adGRP"/$"grpTotal").withColumn("prog_genre",lit("DRAMA/SOAP"))
display(GRP)

// COMMAND ----------

val GRP5=allData.filter($"Programme genre"==="FEATURE FILMS").groupBy("Channel").agg(sum("rat%").alias("grpTotal"),countDistinct("Advertiser").alias ("advertiserCount")).orderBy($"advertiserCount".desc).withColumn("adGRP",($"grpTotal"*1800/10)).withColumn("adGRPAdvertiser",$"adGRP"/$"grpTotal").withColumn("prog_genre",lit("FEATURE FILMS"))
display(GRP5)

// COMMAND ----------

val batch=allData.groupBy("Channel").agg(sum("rat%").alias("grpTotal")).withColumn("adGRP",($"grpTotal"*1800/10))
display(batch)

// COMMAND ----------

val DFList1 = Seq(GRP, GRP1, GRP2,GRP3,GRP4,GRP5)
var allUnionDF1 = DFList1.reduce(_ union _)
display(allUnionDF1)

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

Number of advertisers deal in last FY but not in current FY


// COMMAND ----------

val schema=StructType(Seq(
StructField("Region",StringType),
StructField("Target",StringType),
StructField("Channel",StringType),
StructField("Year Code",StringType),
StructField("Week Desc",StringType),
StructField("Week Day",StringType),
StructField("Date",StringType),
StructField("TimeBand",StringType),
StructField("Impressions'000",StringType),
StructField("Audience'000",StringType),
StructField("Rch'000",StringType),
StructField("Rch%",StringType),
StructField("Rat%",StringType)))

// COMMAND ----------

val inputData="/FileStore/tables/Viacom_Timebands_30Min_Gujarati_20190316_19062019_180116-d473a.txt"

// COMMAND ----------

val rddDf=spark.read.text(inputData).rdd.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

// COMMAND ----------

val df=spark.read.option("header","true").option("delimiter",";").schema(schema).csv(rddDf.map{case Row(x:String)=>x}toDS)

// COMMAND ----------

// DBTITLE 1,Saturday
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.Row
val satData=df.filter($"Week Day"==="Saturday")
val satdf1=satData.groupBy("Channel","Date").agg(sum("Impressions'000").alias("impTotal"),sum("Rat%").alias("grpTotal")).withColumn("GVM",$"impTotal"/1000).withColumn("adGRP",($"grpTotal"*1800/10)).withColumn("WeekDay",lit("Saturday")).drop("impTotal","grpTotal")
//display(df1)
var satRanked=satdf1.withColumn("viewershipRank",rank().over(Window.orderBy($"GVM".desc)))
display(satRanked)

// COMMAND ----------

// DBTITLE 1,Sunday
val sunData=df.filter($"Week Day"==="Sunday")
val sundf1=sunData.groupBy("Channel","Date").agg(sum("Impressions'000").alias("impTotal"),sum("Rat%").alias("grpTotal")).withColumn("GVM",$"impTotal"/1000).withColumn("adGRP",($"grpTotal"*1800/10)).withColumn("WeekDay",lit("Sunday")).drop("impTotal","grpTotal")
//display(df1)
var sunRanked=sundf1.withColumn("viewershipRank",rank().over(Window.orderBy($"GVM".desc)))
display(sunRanked)

// COMMAND ----------

// DBTITLE 1,Monday
val monData=df.filter($"Week Day"==="Monday")
val mondf1=monData.groupBy("Channel","Date").agg(sum("Impressions'000").alias("impTotal"),sum("Rat%").alias("grpTotal")).withColumn("GVM",$"impTotal"/1000).withColumn("adGRP",($"grpTotal"*1800/10)).withColumn("WeekDay",lit("Monday")).drop("impTotal","grpTotal")
//display(df1)
var monRanked=mondf1.withColumn("viewershipRank",rank().over(Window.orderBy($"GVM".desc)))
display(monRanked)

// COMMAND ----------

// DBTITLE 1,Tuesday
val tueData=df.filter($"Week Day"==="Tuesday")
val tuedf1=tueData.groupBy("Channel","Date").agg(sum("Impressions'000").alias("impTotal"),sum("Rat%").alias("grpTotal")).withColumn("GVM",$"impTotal"/1000).withColumn("adGRP",($"grpTotal"*1800/10)).withColumn("WeekDay",lit("Tuesday")).drop("impTotal","grpTotal")
//display(df1)
var tueRanked=tuedf1.withColumn("viewershipRank",rank().over(Window.orderBy($"GVM".desc)))
display(tueRanked)

// COMMAND ----------

// DBTITLE 1,Wednesday
val wedData=df.filter($"Week Day"==="Wednesday")
val wedf1=wedData.groupBy("Channel","Date").agg(sum("Impressions'000").alias("impTotal"),sum("Rat%").alias("grpTotal")).withColumn("GVM",$"impTotal"/1000).withColumn("adGRP",($"grpTotal"*1800/10)).withColumn("WeekDay",lit("Wednesday")).drop("impTotal","grpTotal")
//display(df1)
var wedRanked=wedf1.withColumn("viewershipRank",rank().over(Window.orderBy($"GVM".desc)))
display(wedRanked)

// COMMAND ----------

// DBTITLE 1,Thursday
val thurData=df.filter($"Week Day"==="Thursday")
val thurdf1=thurData.groupBy("Channel","Date").agg(sum("Impressions'000").alias("impTotal"),sum("Rat%").alias("grpTotal")).withColumn("GVM",$"impTotal"/1000).withColumn("adGRP",($"grpTotal"*1800/10)).withColumn("WeekDay",lit("Thursday")).drop("impTotal","grpTotal")
//display(df1)
var thurRanked=thurdf1.withColumn("viewershipRank",rank().over(Window.orderBy($"GVM".desc)))
display(thurdf1)

// COMMAND ----------

// DBTITLE 1,Friday
val friData=df.filter($"Week Day"==="Friday")
val fridf1=friData.groupBy("Channel","Date").agg(sum("Impressions'000").alias("impTotal"),sum("Rat%").alias("grpTotal")).withColumn("GVM",$"impTotal"/1000).withColumn("adGRP",($"grpTotal"*1800/10)).withColumn("WeekDay",lit("Friday")).drop("impTotal","grpTotal")
//display(df1)
var friRanked=fridf1.withColumn("viewershipRank",rank().over(Window.orderBy($"GVM".desc)))
display(friRanked)

// COMMAND ----------

val DFList = Seq(satRanked, sunRanked, monRanked,tueRanked,wedRanked,thurRanked,friRanked)
var allUnionDF = DFList.reduce(_ union _)

// COMMAND ----------

display(allUnionDF)