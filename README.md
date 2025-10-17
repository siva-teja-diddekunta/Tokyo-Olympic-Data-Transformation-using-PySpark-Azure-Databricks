# Tokyo Olympic Data Transformation using PySpark & Azure Databricks

## Project Overview

**Project Title**: Tokyo Olympic Data
**Level**: Beginner


This project demonstrates how to perform data ingestion, transformation, and analysis on the Tokyo Olympic dataset using Azure Databricks and PySpark.
It covers the full pipeline — from mounting Azure Data Lake Storage (ADLS) to reading raw CSVs, inspecting schemas, cleaning data, transforming it, and writing the processed data back to ADLS.

## Project Workflow
1.**Mount Azure Data Lake Storage (ADLS)**

Configured OAuth authentication to securely connect Databricks with Azure Data Lake.

Mounted the container tokyo-olympic-data into Databricks at /mnt/tokyoolymic.

Verified the mount and checked directories:
```
Python
%fs
ls "/mnt/tokyoolymic"
```
__
2. Read Data from ADLS

Loaded multiple CSV datasets from the /raw-data/ folder using PySpark:
```python
athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/medals.csv")
teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymic/raw-data/teams.csv")
```

## Datasets Included

Athletes – Competitor details (name, country, discipline)

Coaches – Coach information with associated events

EntriesGender – Gender participation across disciplines

Medals – Medal counts and rankings by country

Teams – Team participation by event and country

**3. Data Exploration**

Displayed top records using .show()

Inspected schema using .printSchema()

Verified column datatypes and structures

Example:
```Python
athletes.printSchema()
entriesgender.printSchema()
```
**4. Data Cleaning & Type Casting**

Converted gender columns to numeric datatypes for analysis:
```python
entriesgender = entriesgender.withColumn("Female", col("Female").cast(IntegerType())) \
                             .withColumn("Male", col("Male").cast(IntegerType())) \
                             .withColumn("Total", col("Total").cast(IntegerType()))
```
**5. Transformations & Analysis**

**Top Gold Medal Countries**
```python
medals.orderBy("Gold", ascending=False).select("Team_Country","Gold").show()
```
**Average Entries by Gender**
```python
average_entries_by_gender = entriesgender.withColumn(
    'Avg_Female', entriesgender['Female'] / entriesgender['Total']
).withColumn(
    'Avg_Male', entriesgender['Male'] / entriesgender['Total']
)
average_entries_by_gender.show()
```

**6. Write Transformed Data Back to ADLS**

Saved processed datasets into the /transformed-data/ folder:
```python
athletes.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/athletes")
coaches.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/coaches")
entriesgender.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/entriesgender")
medals.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/medals")
teams.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transformed-data/teams")
```
**Tech Stack**
Component	                                 Purpose
Azure Databricks	               Big data processing & transformation
Azure Data Lake (Gen2)	           Cloud storage for raw and processed data
PySpark	                           Distributed data transformation framework
OAuth (Service Principal)	       Secure authentication for ADLS access

## Key Learnings

Setting up secure ADLS mounts using OAuth and Databricks secrets

Reading, cleaning, and writing large CSV files with Spark DataFrames

Performing schema inspection, type casting, and data validation

Using PySpark transformations like withColumn, select, and orderBy

Writing cleaned datasets efficiently back to ADLS

## Folder Structure

**Tokyo-Olympic-Transformation/**
│
├── raw-data/
│   ├── athletes.csv
│   ├── coaches.csv
│   ├── entriesgender.csv
│   ├── medals.csv
│   └── teams.csv
│
├── transformed-data/
│   ├── athletes/
│   ├── coaches/
│   ├── entriesgender/
│   ├── medals/
│   └── teams/
│
└── Tokyo_Olympic_Transformation.dbc

## Future Enhancements

Automate transformation pipeline using Databricks Jobs or Azure Data Factory

Add data validation with Great Expectations

Build Power BI / Tableau dashboards for medal and participation insights

## Author - Sivateja Diddekunta
Data & Analytics Engineer
Turning distributed data into actionable insights across AWS, Azure, and GCP
Skilled in SQL, Python, Spark, Databricks, Snowflake, dbt, and Airflow
Passionate about building scalable, reliable data architectures that bridge engineering and business
