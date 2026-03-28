# Patients-LOS-with-DataBricks
Patients Analytics

```python
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
```

```python
# 1. Define the Raw URL
url = "https://raw.githubusercontent.com/aliadamgit/PatientsRecords/refs/heads/main/encounters.csv"

# 2. Read into Pandas first (simplest for URLs)
pdf = pd.read_csv(url)

# 3. Convert to Spark DataFrame if you need to use Spark features
spark_df = spark.createDataFrame(pdf)

display(spark_df)
```

```python
%sql
CREATE CATALOG IF NOT EXISTS hr_catalog
```

```python
%sql
CREATE SCHEMA IF NOT EXISTS hr_catalog.hr_schema;
```

```python
encounters_df = spark_df.write.mode("overwrite").format("delta").saveAsTable("hr_catalog.hr_schema.encounters")
display(encounters_df)
```

```python
df_encounters = spark.sql("SELECT * FROM hr_catalog.hr_schema.encounters");
```

```python
df_encounters = spark.sql("SELECT * FROM hr_catalog.hr_schema.encounters");
```


```python
from pyspark.sql.functions import to_date, to_timestamp
```

```python
df_encounters = df_encounters.withColumn("START",col("START").cast("timestamp"))\
                             .withColumn("STOP",col("STOP").cast("timestamp"))
display(df_encounters.head(2))
```


```python
# from pyspark.sql.functions import *
LOS = df_encounters.withColumn(
    "los_hours",
    (unix_timestamp("STOP") - unix_timestamp("START")) / 3600
)
```
