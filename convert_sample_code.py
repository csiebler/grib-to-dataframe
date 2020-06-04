# Databricks notebook source
files = dbutils.fs.ls("/mnt/ecmwf")
path = ''
for file in files:
  path = file.path#.replace('dbfs:','/dbfs')
  if path.startswith('dbfs:/mnt/ecmwf/D2'):
    pathTo = path.replace('/D2','/_D2') #rename a file so it doesn't get processed by another instance
    dbutils.fs.mv(path,pathTo)
    path = pathTo.replace('dbfs:','/dbfs')
    print(path)
    break
 
# COMMAND ----------
 
import cfgrib
import xarray as xr
ds = cfgrib.open_datasets(path,backend_kwargs={'read_keys': ['pv'], 'indexpath': ''})
 
# COMMAND ----------
 
import pandas as pd
 
x = ds[0].t.attrs['GRIB_pv']
nl = len(x)
 
if nl == 184: #91 vertical levels
  a = dict(zip(range(92), list(x[0: 92])))
  b = dict(zip(range(92), list(x[92: 184])))
elif nl == 276: #137 vertical levels
  a = dict(zip(range(138), list(x[0: 138])))
  b = dict(zip(range(138), list(x[138: 276])))
else :
  raise Exception("Cannot retrieve a,b parameters for vertical levels!")
 
ab = pd.DataFrame(index = a.keys(), columns = ['A'], data = a.values())
ab['B'] = b.values()
 
# COMMAND ----------
 
for dataset in ds:
  pdf = dataset.to_dataframe()
  pdf.reset_index(inplace=True)
  pdf = pdf.drop(columns='step')
  df = spark.createDataFrame(pdf)
  delta = '/mnt/delta/ecmwf_d2_lnsp' if 'lnsp' in df.columns else '/mnt/delta/ecmwf_d2'
  df.write.format("delta").mode("append").save(delta)
 
# COMMAND ----------
 
from pyspark.sql.functions import lit
 
ab.reset_index(inplace=True)
ab = spark.createDataFrame(ab)
 
# will fill out with data for previously imported d2 files later. keeping a single set for now to save some time
# ab.withColumn('time',lit(df.first().time)).write.format("delta").mode("append").save('/mnt/delta/d2_ab')
 
# COMMAND ----------
 
path = path.replace('/dbfs','')
pathTo = path.replace('ecmwf/','ecmwf/archive/')
dbutils.fs.mv(path,pathTo)