#!/usr/bin/env python
# coding: utf-8

# ## EDA
# 
# New notebook

# In[2]:


# Welcome to your new notebook
# Type here in the cell editor to add code!
# Load table into Spark DataFrame
df = spark.read.table("max.taxi_rides")

# Show first 5 rows
df.show(5)


# In[3]:


df.columns


# ## EDA

# In[4]:


from pyspark.sql import functions as F

# Load the table
df = spark.read.table("taxi_rides")

# Preview a few rows
df.show(10, truncate=False)


# In[5]:


# Schema (column names + types)
df.printSchema()


# In[6]:


# Row & column count
print(f"Rows: {df.count()}, Columns: {len(df.columns)}")


# In[7]:


# Summary stats for numeric columns
df.describe().show()



# In[8]:


# More detailed summary (includes percentiles, min, max)
df.summary().show()


# In[9]:


# Check null values per column
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()


# In[10]:


# Distinct count of values for each column (good for categorical check)
for c in df.columns:
    print(f"{c}: {df.select(c).distinct().count()} distinct values")


# In[11]:


df.groupBy("paymentType").count().orderBy(F.desc("count")).show(10)


# In[12]:


num_cols = [f.name for f in df.schema.fields if f.dataType.typeName() in ("double", "integer", "long")]

for i in range(len(num_cols)):
    for j in range(i+1, len(num_cols)):
        corr = df.stat.corr(num_cols[i], num_cols[j])
        print(f"Correlation({num_cols[i]}, {num_cols[j]}) = {corr}")


# In[13]:


# Example: longest trips
df.orderBy(F.desc("tripDistance")).select("tripDistance", "lpepPickupDatetime", "lpepDropoffDatetime").show(10)

# Example: highest fares
df.orderBy(F.desc("fareAmount")).select("fareAmount", "paymentType").show(10)


# ## Data Cleaning

# In[14]:


# --- 1. Drop useless columns ---
df = df.drop("ehailFee")   # Always null


# In[15]:


# --- 2. Handle categorical fields ---
df = (df
      .withColumn("paymentType",
                  F.when(F.col("paymentType") == 1, "Credit Card")
                   .when(F.col("paymentType") == 2, "Cash")
                   .when(F.col("paymentType") == 3, "No Charge")
                   .when(F.col("paymentType") == 4, "Dispute")
                   .when(F.col("paymentType") == 5, "Unknown")
                   .otherwise("Other"))
      .withColumn("storeAndFwdFlag",
                  F.when(F.col("storeAndFwdFlag") == "Y", "Yes")
                   .when(F.col("storeAndFwdFlag") == "N", "No")
                   .otherwise("Unknown"))
      .withColumn("tripType",
                  F.when(F.col("tripType") == 1, "Street-hail")
                   .when(F.col("tripType") == 2, "Dispatch")
                   .otherwise("Unknown"))
     )


# In[16]:


# --- 3. Feature engineering ---
df = (df
      # Trip duration (minutes)
      .withColumn("tripDurationMinutes", 
                  (F.col("lpepDropoffDatetime").cast("long") -
                   F.col("lpepPickupDatetime").cast("long"))/60)
      # Time breakdown
      .withColumn("year", F.year("lpepPickupDatetime"))
      .withColumn("month", F.month("lpepPickupDatetime"))
      .withColumn("day", F.dayofmonth("lpepPickupDatetime"))
      .withColumn("dayOfWeek", F.date_format("lpepPickupDatetime", "E"))
      .withColumn("hour", F.hour("lpepPickupDatetime"))
      # Tip %
      .withColumn("tipPercent", 
                  F.when(F.col("fareAmount") > 0, 
                         (F.col("tipAmount")/F.col("fareAmount"))*100)
                  .otherwise(0))
     )


# In[17]:


# --- 4. Data quality filters ---
df = (df
      .filter(F.col("passengerCount").between(1,6))
      .filter(F.col("fareAmount") > 0)
      .filter(F.col("totalAmount") > 0)
      .filter(F.col("tripDistance") > 0)
      .filter(F.col("tripDistance") < 100) # cap to remove outliers
      .filter(F.col("tripDurationMinutes") > 0)
     )


# In[18]:


# --- 5. Aggregate for geospatial analysis ---
# Pickup heatmap prep
pickup_heatmap = (df.groupBy("puLocationId")
                  .agg(F.count("*").alias("pickup_count")))

# Dropoff heatmap prep
dropoff_heatmap = (df.groupBy("doLocationId")
                   .agg(F.count("*").alias("dropoff_count")))

# Join them back
zone_stats = (pickup_heatmap
              .join(dropoff_heatmap, 
                    pickup_heatmap.puLocationId == dropoff_heatmap.doLocationId, 
                    "outer")
              .withColumnRenamed("puLocationId", "LocationId"))


# In[19]:


# --- 6. Save cleaned table back to Lakehouse ---
(df.write
   .mode("overwrite")
   .format("delta")
   .saveAsTable("max.taxi_rides_cleaned"))

(zone_stats.write
   .mode("overwrite")
   .format("delta")
   .saveAsTable("max.taxi_rides_zonestats"))


# In[ ]:


# View first 10 rows of cleaned taxi rides
spark.table("max.taxi_rides_cleaned").show(10, truncate=False)


# In[ ]:


# View first 10 rows of pickup/dropoff zone stats
spark.table("max.taxi_rides_zonestats").show(10, truncate=False)


# In[20]:


# Load the tables
df_cleaned = spark.table("taxi_rides_cleaned")
df_zonestats = spark.table("taxi_rides_zonestats")

# Filter out rows where doLocationId is NULL
df_cleaned_filtered = df_cleaned.filter("doLocationId IS NOT NULL")
df_zonestats_filtered = df_zonestats.filter("doLocationId IS NOT NULL")

# Overwrite the tables
df_cleaned_filtered.write.format("delta").mode("overwrite").saveAsTable("taxi_rides_cleaned")
df_zonestats_filtered.write.format("delta").mode("overwrite").saveAsTable("taxi_rides_zonestats")


# In[24]:


# Row count
row_count = df_zonestats_filtered.count()

# Column count
col_count = len(df_zonestats_filtered.columns)

print(f"Shape: ({row_count}, {col_count})")


# In[28]:


df_cleaned_filtered.show(5)  # or .show()


# In[30]:


df = spark.sql("SELECT * FROM max.taxi_rides_cleaned LIMIT 20")
display(df)

