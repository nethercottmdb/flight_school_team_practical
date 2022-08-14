# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ./resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

# DBTITLE 1,How to implement and run this notebook
#When possible, always use one of the SHARED-XXX cluster available
#Tips: the %run cell defined above init a database with your name, and Python path variable is available, use it to store intermediate data or your checkpoints
#You need to run the setup cell to have these variable defined:
print(f"path={path}")
#Just save create the database to the current database, it's been initiliazed locally to your user to avoid conflict
print("your current database has been initialized to:")
print(sql("SELECT current_database() AS db").collect()[0]['db'])

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #YOUR DEMO INTRO HERE
# MAGIC 
# MAGIC TODO: use this cell to present your demo at a high level. What are you building ? What's your story ? How is it linked to megacorp powerplant and its gaz turbine ?
# MAGIC 
# MAGIC ### Story example: 
# MAGIC 
# MAGIC - We'll be ingesting Turbine IOT sensor data (vibrations, speed etc) from our Power Plant (this notebook).
# MAGIC - With this data, we can build a ML model to predict when a turbine is defective and send a maintenance team to avoid outage
# MAGIC - We'll be able to build a Dashboard for the Business team to track our Power Plant efficiency and potential technical issues, with a estimation of the impact/gain to fix the equiment (in $)!
# MAGIC 
# MAGIC `display_slide('1KvjJCUgie81u-0lb4bG2Fivd6qEzgjC3jhq46NpDyls', '11')`
# MAGIC 
# MAGIC Need to display images ? check https://docs.databricks.com/data/filestore.html#filestore

# COMMAND ----------

display_slide('137acXM8aj9dJUZbLpH3ZIorKHzBx3B0wqPRm-qdQ-yU', 38)  #hide this code

# COMMAND ----------

# MAGIC %md 
# MAGIC TODO: what's the data flow ? What will you do in more detail here for the Data Engineering part?
# MAGIC Tips: check this example: https://docs.google.com/presentation/d/137acXM8aj9dJUZbLpH3ZIorKHzBx3B0wqPRm-qdQ-yU/edit#slide=id.gf3af1439a3_0_54

# COMMAND ----------

display_slide('137acXM8aj9dJUZbLpH3ZIorKHzBx3B0wqPRm-qdQ-yU', 39)  #hide this code

# COMMAND ----------

Our raw data is made available as files in a bucket mounted under /mnt/field-demos/manufacturing/iot_turbine/incoming-data-json
TODO: Use %fs to visualize the incoming data under /mnt/field-demos/manufacturing/iot_turbine/incoming-data-json

# COMMAND ----------

# MAGIC %sql
# MAGIC --TODO : Select and display the entire incoming json data using a simple SQL:
# MAGIC SELECT * FROM json.`/your/location/xxx`
# MAGIC 
# MAGIC -- What you have here is a list of IOT sensor metrics (AN1,AN2...) that you get from your turbine (vibration, speed...).
# MAGIC -- We'll then ingest these metrics and use them to detect when a turbine isn't healthy, so that we can prevent outage. 
# MAGIC 
# MAGIC -- TODO: take some time to understand the data, and keep it super simple (you can choose what AN1/2/3 represent for megacorp).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ Bronze layer: ingesting incremental data as a stream

# COMMAND ----------

# DBTITLE 1,Stream landing files from cloud storage
# Set up the stream to begin reading incoming files from the mount point.

#TODO: ingest data using cloudfile
#Take some time to review the documentation: https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html#quickstart

#Incoming data is available under /mnt/field-demos/manufacturing/iot_turbine/incoming-data-json
#Goal: understand autoloader value and functionality (schema evolution, inference)
#What your customer challenges could be with schema evolution, schema inference, incremental mode having lot of small files? 
#How do you fix that ?

#load the stream with pyspark readstrem and autoloader
#bronzeDF = spark.readStream .....
#Tips: use .option("cloudFiles.maxFilesPerTrigger", 1) to consume 1 file at a time and simulate a stream during the demo
#Tips: use your local path to save the scheam: .option("cloudFiles.schemaLocation", path+"/schema_bronze")    

#TODO: write the output as "turbine_bronze" delta table, with a trigger of 10 seconds or a trigger Once
# Write Stream as Delta Table
#bronzeDF.writeStream ...
#Tips: use your local path to save the checkpoint: .option("checkpointLocation", path+"/bronze_checkpoint")    
#Tips: if you're looking for your table in the data explorer, they're under the database having your name (see init cell)

# COMMAND ----------

# DBTITLE 1,Our raw data is now available in a Delta table
# MAGIC %sql
# MAGIC -- you should have a bronze table structured with the following columns: ID AN3 AN4 AN5 AN6 AN7 AN8 AN9 AN10 SPEED TORQUE _rescued
# MAGIC -- TODO: run a SELECT from your turbine_bronze

# COMMAND ----------

# DBTITLE 1,Fixing 
# MAGIC %sql
# MAGIC -- TODO: which table property should you define to solve small files issue ? What's the typical challenge running streaming operation? And the value for your customer.
# MAGIC -- Documentation: https://docs.databricks.com/delta/optimizations/auto-optimize.html
# MAGIC -- ALTER TABLE ....

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2/ Silver layer: cleanup data and remove unecessary column

# COMMAND ----------

#TODO: cleanup the silver table
#Our bronze silver should have TORQUE with mostly NULL value and the _rescued column should be empty.
#drop the TORQUE column, filter on _rescued to select only the rows without json error from the autoloader, filter on ID not null as you'll need it for your join later
from pyspark.sql.functions import col

silverDF = spark.readStream.table('turbine_bronze') ....
#TODO: cleanup the data, make sure all IDs are not null and _rescued is null (if it's not null it means we couldn't parse the json with the infered schema).

silverDF.writeStream ...
#TODO: write it back to your "turbine_silver" table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: run a SELECT from your turbine_silver table

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Gold layer: join information on Turbine status to add a label to our dataset

# COMMAND ----------

#TODO: the turbine status is available under /mnt/field-demos/manufacturing/iot_turbine/status as parquet file
# Use dbutils.fs to display the folder content

# COMMAND ----------

#TODO:
#Display the parquet available in folder content using standard spark read
display(spark.read...)

# COMMAND ----------

# MAGIC %sql
# MAGIC --TODO: save the status data as our turbine_status table
# MAGIC --Use databricks COPY INTO COMMAND https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-copy-into.html
# MAGIC --Tips: as of DBR 10.3, schema inference isn't available with cloudfile reading parquet. If you chose to use autoloader instead of the COPY INTO command to load the status, you'll have to specify the schema. 
# MAGIC COPY INTO turbine_status FROM ...

# COMMAND ----------

# DBTITLE 1,Join data with turbine status (Damaged or Healthy)
turbine_stream = spark.readStream.table('turbine_silver')
turbine_status = spark.read.table("turbine_status")

#TODO: do a left join between turbine_stream and turbine_status on the 'id' key and save back the result as the "turbine_gold" table
turbine_stream.join(....

# COMMAND ----------

# MAGIC %sql
# MAGIC --Our turbine gold table should be up and running!
# MAGIC select TIMESTAMP, AN3, SPEED, status from turbine_gold;

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Run DELETE/UPDATE/MERGE with DELTA ! 
# MAGIC We just realized that something is wrong in the data before 2020-05-10 00:00:00! Let's DELETE all this data from our gold table as we don't want to have wrong value in our dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: DELETE all data from before '2020-01-01' in turbine_gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: show some Delta Love.
# MAGIC -- What's unique to Delta and how can it be usefull for your customer (Timetravel? Clone? CDC if you feel adventurous)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant Access to Database
# MAGIC 
# MAGIC Our data is now available. We can easily grant READ access to the Data Science and Data Analyst team!
# MAGIC *Note: (If on a Table-ACLs enabled High-Concurrency Cluster or using UC (coming soon!))*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Note: this won't work with standard cluster. 
# MAGIC -- DO NOT try to make it work during the demo (you need UC)
# MAGIC -- Understand what's required as of now (which cluster type) and the implications
# MAGIC -- explore Databricks Unity Catalog initiative (go/uc) 
# MAGIC 
# MAGIC GRANT SELECT ON DATABASE turbine_demo TO `data.scientist@databricks.com`
# MAGIC GRANT SELECT ON DATABASE turbine_demo TO `data.analyst@databricks.com`

# COMMAND ----------

# MAGIC %md ### What's next
# MAGIC 
# MAGIC TODO: wrapup on the Lakehouse concept. What have we done here?
# MAGIC What's the next step?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Don't forget to Cancel all the streams once your demo is over

# COMMAND ----------

for s in spark.streams.active:
  s.stop()
