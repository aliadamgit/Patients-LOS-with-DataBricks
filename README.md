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
