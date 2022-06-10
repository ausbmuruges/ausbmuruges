# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT logicalenv DEFAULT "prd001";

# COMMAND ----------

# MAGIC %python
# MAGIC pip install pyproj

# COMMAND ----------

# MAGIC %python
# MAGIC import pyproj
# MAGIC def st_x(x,y):
# MAGIC   p = pyproj.Proj("+proj=lcc +lat_0=-37 +lon_0=145 +lat_1=-36 +lat_2=-38 +x_0=1000000 +y_0=1000000 +ellps=GRS80 +units=mm +no_defs")
# MAGIC   x,y = p(x, y, inverse=True)
# MAGIC   return(x)
# MAGIC spark.udf.register("st_x", st_x)
# MAGIC 
# MAGIC def st_y(x,y):
# MAGIC   p = pyproj.Proj("+proj=lcc +lat_0=-37 +lon_0=145 +lat_1=-36 +lat_2=-38 +x_0=1000000 +y_0=1000000 +ellps=GRS80 +units=mm +no_defs")
# MAGIC   x,y = p(x, y, inverse=True)
# MAGIC   return(y)
# MAGIC spark.udf.register("st_y", st_y)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     he.EQUIPMENT_ID AS LIGHT_ID,
# MAGIC 	trim(sss.MANUFACTURER) AS MANUFACTURER,
# MAGIC 	sss.MODEL,
# MAGIC 	sss.LAMP_TYPE,
# MAGIC 	cast(sss.WATTS_RATING AS DECIMAL(12, 1)) AS WATTS_RATING,
# MAGIC 	cast(sss.AEMO_WATTS_RATING AS DECIMAL(12, 1)) AS AEMO_WATTS_RATING,
# MAGIC 	sss.TENNON_TYPE,
# MAGIC 	sse.NMA_CONTROL_GEAR AS CONTROL_GEAR,
# MAGIC 	CASE 
# MAGIC 		WHEN sss.real_spec = 'true'
# MAGIC 			THEN 'Standard'
# MAGIC 		WHEN sss.real_spec = 'false'
# MAGIC 			THEN 'Non-Standard'
# MAGIC 		ELSE 'Non-Standard'
# MAGIC 		END AS REAL_SPEC,
# MAGIC 	sse.DATE_INSTALLED AS CONNECTION_DATE,
# MAGIC 	sse.NMA_DATE_LAMP_CHANGED AS GLOBE_CHANGE_DATE,
# MAGIC 	sse.NMA_DATE_BILLING_CHANGED AS BILLING_EFF_DATE,
# MAGIC 	sse.OWNER_NAME,
# MAGIC 	sse.NMA_BILLING_STATUS AS BILLING_STATUS,
# MAGIC 	sse.STATUS AS LC_STATUS,
# MAGIC     *
# MAGIC FROM im${logicalenv}_rawvault.hub_equipment he
# MAGIC INNER JOIN im${logicalenv}_rawvault.lnk_equipment_specification les ON he.HUB_EQUIPMENT_HSH_KEY = les.HUB_EQUIPMENT_HSH_KEY
# MAGIC INNER JOIN im${logicalenv}_rawvault.hub_specification hs ON les.HUB_SPECIFICATION_HSH_KEY = hs.HUB_SPECIFICATION_HSH_KEY
# MAGIC INNER JOIN im${logicalenv}_rawvault.sat_specification_sde_light_spec_cv sss ON hs.HUB_SPECIFICATION_HSH_KEY = sss.HUB_SPECIFICATION_HSH_KEY
# MAGIC INNER JOIN im${logicalenv}_rawvault.SAT_EQUIPMENT_SDME_EO_LIGHT_CV sse ON he.HUB_EQUIPMENT_HSH_KEY = sse.HUB_EQUIPMENT_HSH_KEY
# MAGIC where he.EQUIPMENT_ID = '2380495'    

# COMMAND ----------

# MAGIC %sql
# MAGIC select LIGHT_ID, count(9) from (
# MAGIC SELECT CASE 
# MAGIC 		WHEN trim(sss.MANUFACTURER) = 'Artcraft'
# MAGIC 			THEN '6305010117'
# MAGIC 		WHEN trim(sss.MANUFACTURER) = 'Braums'
# MAGIC 			THEN '6305010121'
# MAGIC 		WHEN trim(sss.MANUFACTURER) = 'Philips'
# MAGIC 			THEN '6305010143'
# MAGIC 		WHEN trim(sss.MANUFACTURER) = 'Vicpole'
# MAGIC 			THEN '6305010154'
# MAGIC 		WHEN trim(sss.MANUFACTURER) = 'Sylvania'
# MAGIC 			THEN '6305010171'
# MAGIC 		ELSE '6305010177'
# MAGIC 		END AS NMI,
# MAGIC 	he.EQUIPMENT_ID AS LIGHT_ID,
# MAGIC 	CASE 
# MAGIC 		WHEN trim(sss.MANUFACTURER) = 'Artcraft'
# MAGIC 			THEN '6305010177'
# MAGIC 		END AS COST_SHARE_NMI,
# MAGIC 	CASE 
# MAGIC 		WHEN trim(sss.MANUFACTURER) = 'Artcraft'
# MAGIC 			THEN 40
# MAGIC 		END COST_SHARE_PERCENT,
# MAGIC 	trim(sss.MANUFACTURER) AS MANUFACTURER,
# MAGIC 	sss.MODEL,
# MAGIC 	sss.LAMP_TYPE,
# MAGIC 	cast(sss.WATTS_RATING AS DECIMAL(12, 1)) AS WATTS_RATING,
# MAGIC 	cast(sss.AEMO_WATTS_RATING AS DECIMAL(12, 1)) AS AEMO_WATTS_RATING,
# MAGIC 	sss.TENNON_TYPE,
# MAGIC 	sse.NMA_CONTROL_GEAR AS CONTROL_GEAR,
# MAGIC 	CASE 
# MAGIC 		WHEN sss.real_spec = 'true'
# MAGIC 			THEN 'Standard'
# MAGIC 		WHEN sss.real_spec = 'false'
# MAGIC 			THEN 'Non-Standard'
# MAGIC 		ELSE 'Non-Standard'
# MAGIC 		END AS REAL_SPEC,
# MAGIC 	sse.DATE_INSTALLED AS CONNECTION_DATE,
# MAGIC 	sse.NMA_DATE_LAMP_CHANGED AS GLOBE_CHANGE_DATE,
# MAGIC 	sse.NMA_DATE_BILLING_CHANGED AS BILLING_EFF_DATE,
# MAGIC 	cast(st_y(sseg.latr, sseg.lonr) AS DECIMAL(21, 15)) AS LOCATION_LAT,
# MAGIC 	cast(st_x(sseg.latr, sseg.lonr) AS DECIMAL(21, 15)) AS LOCATION_LONG,
# MAGIC 	sse.OWNER_NAME,
# MAGIC 	sse.NMA_BILLING_STATUS AS BILLING_STATUS,
# MAGIC 	sse.STATUS AS LC_STATUS,
# MAGIC 	date_format(from_utc_timestamp(current_timestamp(), 'GMT+10'), 'yyyy-MM-dd HH:mm:ss') AS ROW_INSERT_DTM
# MAGIC FROM im${logicalenv}_rawvault.hub_equipment he
# MAGIC INNER JOIN im${logicalenv}_rawvault.lnk_equipment_specification les ON he.HUB_EQUIPMENT_HSH_KEY = les.HUB_EQUIPMENT_HSH_KEY
# MAGIC INNER JOIN im${logicalenv}_rawvault.hub_specification hs ON les.HUB_SPECIFICATION_HSH_KEY = hs.HUB_SPECIFICATION_HSH_KEY
# MAGIC INNER JOIN im${logicalenv}_rawvault.sat_specification_sde_light_spec_cv sss ON hs.HUB_SPECIFICATION_HSH_KEY = sss.HUB_SPECIFICATION_HSH_KEY
# MAGIC INNER JOIN im${logicalenv}_rawvault.SAT_EQUIPMENT_SDME_EO_LIGHT_CV sse ON he.HUB_EQUIPMENT_HSH_KEY = sse.HUB_EQUIPMENT_HSH_KEY
# MAGIC INNER JOIN (
# MAGIC 	SELECT HUB_EQUIPMENT_HSH_KEY,
# MAGIC 		substring_index(regexp_replace(LOCATION, "[POINT()]", ""), ' ', '1') latr,
# MAGIC 		substring_index(regexp_replace(LOCATION, "[POINT()]", ""), ' ', '-1') lonr,
# MAGIC 		rank() OVER (
# MAGIC 			PARTITION BY HUB_EQUIPMENT_HSH_KEY ORDER BY EXTRACT_DATE_TIME DESC
# MAGIC 			) AS RNK
# MAGIC 	FROM im${logicalenv}_rawvault.SAT_EQUIPMENT_SDME_EO_LIGHT_GEOM_CV
# MAGIC 	) sseg ON he.HUB_EQUIPMENT_HSH_KEY = sseg.HUB_EQUIPMENT_HSH_KEY
# MAGIC     ) group by LIGHT_ID
# MAGIC     having count(9) > 1

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.help()

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.notebook.help()

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")

# COMMAND ----------

# MAGIC %scala
# MAGIC print(spark.conf.getAll)

# COMMAND ----------

env_sql = """show databases like '*raw*'"""

