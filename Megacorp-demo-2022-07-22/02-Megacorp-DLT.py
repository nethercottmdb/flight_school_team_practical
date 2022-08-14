# Databricks notebook source
# MAGIC %md #Introducing Delta Live Table
# MAGIC 
# MAGIC TODO: Explain the typical challenges a customer will face without DLT
# MAGIC 
# MAGIC TODO: Explain what DLT is adding as value for the customer
# MAGIC 
# MAGIC Tips: include a slide if needed ! use http://go/dlt to get internal DLT content
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/iot-wind-turbine/resources/images/iot-turbine-flow-0.png" width="1000"/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Ingest raw data with autoloader
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/iot-wind-turbine/resources/images/iot-turbine-flow-1.png" width="600" style="float: right" />
# MAGIC 
# MAGIC -- TODO: Pitch autoloader value?

# COMMAND ----------

#TODO
#In SQL, rewrite the transformations from notebook 01-Megacorp-data-ingestion using DLT, and building the DLT graph
#Add some expectations
#go in Job => DLT, create your DLT pointing to your notebook
#help: https://docs.databricks.com/data-engineering/delta-live-tables/index.html
#Remember: you can use the retail DLT as example

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Cleaning data with expectations
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/iot-wind-turbine/resources/images/iot-turbine-flow-2.png" width="600" style="float: right" />
# MAGIC 
# MAGIC -- TODO: Pitch expectations?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Ingest Gas turbine status information
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/iot-wind-turbine/resources/images/iot-turbine-flow-3.png" width="600" style="float: right" />
# MAGIC 
# MAGIC -- TODO: Pitch autoloader value?

# COMMAND ----------



# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Join table to create gold layer available for Dashboarding & ML
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/iot-wind-turbine/resources/images/iot-turbine-flow-4.png" width="600" style="float: right" />
# MAGIC 
# MAGIC -- TODO: Pitch lakehouse, grant SQL access to other team, 1 copy of the data?

# COMMAND ----------

#TODO: review the the expectation dashboard and how the data can be used to track ingestion quality
#more details: https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#notebook/3469214860228002/command/3418422060417474
#data quality tracker dashboard: https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/6f73dd1b-17b1-49d0-9a11-b3772a2c3357-dlt---retail-data-quality-stats?o=1444828305810485
#do not try to reproduce a dashboard for this specific use-case

# COMMAND ----------

# MAGIC %md
# MAGIC # Checking your data quality metrics with Delta Live Tables
# MAGIC Delta Live Tables tracks all your data quality metrics. You can leverage the expecations directly as SQL table with Databricks SQL to track your expectation metrics and send alerts as required. This let you build the following dashboards:
# MAGIC 
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">
# MAGIC 
# MAGIC <a href="https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/6f73dd1b-17b1-49d0-9a11-b3772a2c3357-dlt---retail-data-quality-stats?o=1444828305810485" target="_blank">Data Quality Dashboard</a>
