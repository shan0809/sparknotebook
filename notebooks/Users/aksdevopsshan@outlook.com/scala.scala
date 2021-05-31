// Databricks notebook source
// MAGIC %md
// MAGIC #Create MountPoints, Read the data and push it to Azure blob storage

// COMMAND ----------

val containerName = "codered"
val storageAccountName = "azprblob"
val sas = "*************"
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
 .save("/mnt/result/SalesProfitData1.csv")

// COMMAND ----------

// MAGIC %sh git clone git@github.com:shan0809/Jumpstart-Terraform-0.12-on-Azure.git

// COMMAND ----------

