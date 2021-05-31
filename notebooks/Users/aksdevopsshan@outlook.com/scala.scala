// Databricks notebook source
// MAGIC %md
// MAGIC #Create MountPoints, Read the data and push it to Azure blob storage

// COMMAND ----------

val containerName = "codered"
val storageAccountName = "azprblob"
val sas = "?sv=2020-02-10&ss=bfqt&srt=sco&sp=rwdlacuptfx&se=2021-06-01T02:23:35Z&st=2021-05-31T18:23:35Z&spr=https&sig=4uVjpFDKWeHzB%2FTlz6l8IayNLUI3TLSS9HWsRnC01LI%3D"
val config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"
dbutils.fs.mount(
  source = "wasbs://codered@azprblob.blob.core.windows.net",
  mountPoint = "/mnt/datastore",
  extraConfigs = Map(config -> sas))


val mydf = spark.read
.option("header","true")
.option("inferSchema", "true")
.csv("/mnt/datastore")
display(mydf)


val selectspecificcolsdf = mydf.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME","count")
display(selectspecificcolsdf)

val renamedColsmyDF = selectspecificcolsdf.withColumnRenamed("DEST_COUNTRY_NAME", "SalesPlatform")
display(renamedColsmyDF)
renamedColsmyDF.createOrReplaceTempView("test")

val aggdata = spark.sql("""
SELECT ORIGIN_COUNTRY_NAME, SalesPlatform, sum(count)  From test
group by ORIGIN_COUNTRY_NAME, SalesPlatform
order by SUM(count)
""")

 aggdata.write
 .option("header", "true")
 .format("com.databricks.spark.csv")
 .save("/mnt/result/SalesProfitData2.csv")

// COMMAND ----------

