# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT logicalenv DEFAULT "prd001"

# COMMAND ----------

# MAGIC %sql
# MAGIC select T1.*
# MAGIC FROM imprd001_rawvault.LSAT_NMI_SUBSTATION_POF_NMI_TO_SUBSTATION_DATA T1
# MAGIC JOIN imprd001_rawvault.LNK_NMI_SUBSTATION LNK on T1.LNK_NMI_SUBSTATION_HSH_KEY=LNK.LNK_NMI_SUBSTATION_HSH_KEY
# MAGIC JOIN imprd001_rawvault.HUB_NMI hub
# MAGIC on hub.HUB_NMI_HSH_KEY = LNK.HUB_NMI_HSH_KEY
# MAGIC AND hub.NMI ='6305436859'

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables from im${logicalenv}_rawvault like '*POWER*'

# COMMAND ----------

pip install pyproj

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
# MAGIC with 
# MAGIC SERVICE_POINT_V1 as
# MAGIC ( select servpoint.*
# MAGIC     from ( select lsat.*
# MAGIC                 , ROW_NUMBER() over ( partition by LNK_NMI_SUBSTATION_HSH_KEY order by EXTRACT_DATE_TIME desc, IM_LAST_UPDATE desc ) as `rec_seq`
# MAGIC              from im${logicalenv}_rawvault.LSAT_NMI_SUBSTATION_SDME_EO_SERVICE_POINT lsat
# MAGIC             where date_format(lsat.EXTRACT_DATE_TIME,"yyyyMMdd") < date_format('2022-05-10',"yyyyMMdd")
# MAGIC          ) servpoint
# MAGIC where servpoint.rec_seq = 1
# MAGIC   and servpoint.CHANGE_TYPE != 'D'
# MAGIC ),
# MAGIC GEOM_LOCATION_V2 as
# MAGIC (
# MAGIC SELECT LNK_NMI_SUBSTATION_HSH_KEY
# MAGIC      , EXTRACT_DATE_TIME AS SOURCE_EXTRACT_DTM
# MAGIC      , LOCATION
# MAGIC 	 , substring_index(regexp_replace(LOCATION, "[POINT()]", ""), ' ', '1') latr
# MAGIC 	 , substring_index(regexp_replace(LOCATION, "[POINT()]", ""), ' ', '-1') lonr
# MAGIC 
# MAGIC FROM (select   sat.*
# MAGIC              , ROW_NUMBER() OVER ( partition BY sat.LNK_NMI_SUBSTATION_HSH_KEY ORDER BY sat.extract_date_time DESC) as `rec_seq`
# MAGIC         from im${logicalenv}_rawvault.LSAT_NMI_SUBSTATION_SDME_EO_SERVICE_POINT_GEOM sat
# MAGIC        WHERE date_format(sat.EXTRACT_DATE_TIME,"yyyyMMdd") < date_format('2022-05-10',"yyyyMMdd")
# MAGIC       )lsat
# MAGIC where lsat.rec_seq = 1
# MAGIC   and lsat.change_type <> 'D'
# MAGIC ),
# MAGIC JOINED_VIEW AS (
# MAGIC select hub_nmi.NMI
# MAGIC      , hub_sub.SUBSTATION_ID                  
# MAGIC      , V1.EXTRACT_DATE_TIME    as SOURCE_EXTRACT_DTM
# MAGIC 	   , V2.LOCATION
# MAGIC      , cast(st_y(V2.latr, V2.lonr) AS DECIMAL(21, 15)) AS LOCATION_LATITUDE
# MAGIC      , cast(st_x(V2.latr, V2.lonr) AS DECIMAL(21, 15)) AS LOCATION_LONGITUDE
# MAGIC      
# MAGIC   from SERVICE_POINT_V1 V1
# MAGIC   left join GEOM_LOCATION_V2 V2
# MAGIC     on V1.LNK_NMI_SUBSTATION_HSH_KEY = V2.LNK_NMI_SUBSTATION_HSH_KEY
# MAGIC    join im${logicalenv}_rawvault.LNK_NMI_SUBSTATION lnk
# MAGIC     on lnk.LNK_NMI_SUBSTATION_HSH_KEY = V1.LNK_NMI_SUBSTATION_HSH_KEY
# MAGIC     
# MAGIC   join im${logicalenv}_rawvault.HUB_NMI hub_nmi
# MAGIC     on lnk.HUB_NMI_HSH_KEY= hub_nmi.HUB_NMI_HSH_KEY
# MAGIC     
# MAGIC   join im${logicalenv}_rawvault.HUB_SUBSTATION hub_sub
# MAGIC     on lnk.HUB_SUBSTATION_HSH_KEY= hub_sub.HUB_SUBSTATION_HSH_KEY 
# MAGIC 
# MAGIC 	)
# MAGIC SELECT temp.NMI
# MAGIC      , temp.SUBSTATION_ID                  
# MAGIC      , temp.SOURCE_EXTRACT_DTM
# MAGIC      , temp.LOCATION
# MAGIC      , temp.LOCATION_LATITUDE
# MAGIC      , temp.LOCATION_LONGITUDE
# MAGIC   FROM (
# MAGIC         SELECT JOINED_VIEW.*
# MAGIC              , ROW_NUMBER() OVER(PARTITION BY NMI ORDER BY SOURCE_EXTRACT_DTM DESC ) as rk
# MAGIC           FROM JOINED_VIEW
# MAGIC        ) temp
# MAGIC   where temp.NMI like '6305356889%' 

# COMMAND ----------

# MAGIC %sql
# MAGIC select hub_nmi.*, sha1(concat(nmi,':')), sha1('6305356889:') from im${logicalenv}_rawvault.HUB_NMI hub_nmi where NMI = '317066723'

# COMMAND ----------

# MAGIC %sql
# MAGIC select hub_nmi.*, sha1(concat(nmi,':')) from im${logicalenv}_rawvault.HUB_NMI hub_nmi where NMI = '6305356889'

# COMMAND ----------

# MAGIC %sql
# MAGIC select lnk.*, sha1('6305356889:'), sha1('2602086128:'), sha1(concat('6305356889:', '2602086128:')) from im${logicalenv}_rawvault.LNK_NMI_SUBSTATION lnk where HUB_NMI_HSH_KEY = '5fedc218f314aab6370d5254afaadb441bb2dfa2'

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize imprd001_rawvault.lnk_nmi_substation

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from im${logicalenv}_rawvault.LNK_NMI_SUBSTATION lnk where 
# MAGIC LNK_NMI_SUBSTATION_HSH_KEY = '71516e1dfdab659173ac01292b65e104f374ef25'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from im${logicalenv}_rawvault.HUB_SUBSTATION hs where 
# MAGIC HUB_SUBSTATION_HSH_KEY = '1f36cf68416f6cf653d2bbdf62b6b0438f34b6a0'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from im${logicalenv}_rawvault.LSAT_NMI_SUBSTATION_SDME_EO_SERVICE_POINT where LNK_NMI_SUBSTATION_HSH_KEY = '71516e1dfdab659173ac01292b65e104f374ef25'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from im${logicalenv}_rawvault.LSAT_NMI_SUBSTATION_SDME_EO_SERVICE_POINT where IM_LAST_UPDATE like '2022-05-04%' order by EXTRACT_DATE_TIME desc

# COMMAND ----------

# MAGIC %sql
# MAGIC with 
# MAGIC SERVICE_POINT_V1 as
# MAGIC ( select servpoint.*
# MAGIC     from ( select lsat.*
# MAGIC                 , ROW_NUMBER() over ( partition by LNK_NMI_SUBSTATION_HSH_KEY order by EXTRACT_DATE_TIME desc, IM_LAST_UPDATE desc ) as `rec_seq`
# MAGIC              from im${logicalenv}_rawvault.LSAT_NMI_SUBSTATION_SDME_EO_SERVICE_POINT lsat
# MAGIC             where date_format(lsat.EXTRACT_DATE_TIME,"yyyyMMdd") < date_format('2022-05-09',"yyyyMMdd")
# MAGIC          ) servpoint
# MAGIC where servpoint.rec_seq = 1
# MAGIC   and servpoint.CHANGE_TYPE != 'D'
# MAGIC )
# MAGIC select * from SERVICE_POINT_V1 where LNK_NMI_SUBSTATION_HSH_KEY = '4aee63a9870c5cd870e6a6c115f36369906eedb7'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT LNK_NMI_SUBSTATION_HSH_KEY
# MAGIC      , EXTRACT_DATE_TIME AS SOURCE_EXTRACT_DTM
# MAGIC      , LOCATION
# MAGIC 	 , substring_index(regexp_replace(LOCATION, "[POINT()]", ""), ' ', '1') latr
# MAGIC 	 , substring_index(regexp_replace(LOCATION, "[POINT()]", ""), ' ', '-1') lonr
# MAGIC 
# MAGIC FROM (select   sat.*
# MAGIC              , ROW_NUMBER() OVER ( partition BY sat.LNK_NMI_SUBSTATION_HSH_KEY ORDER BY sat.extract_date_time DESC) as `rec_seq`
# MAGIC         from im${logicalenv}_rawvault.LSAT_NMI_SUBSTATION_SDME_EO_SERVICE_POINT_GEOM sat
# MAGIC        WHERE date_format(sat.EXTRACT_DATE_TIME,"yyyyMMdd") < date_format('2022-05-09',"yyyyMMdd")
# MAGIC       )lsat
# MAGIC where lsat.rec_seq = 1
# MAGIC   and lsat.change_type <> 'D'
# MAGIC   and LNK_NMI_SUBSTATION_HSH_KEY = '4aee63a9870c5cd870e6a6c115f36369906eedb7'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from im${logicalenv}_busvault.bsat_site_info_sdme_cv where NMI = '6305436859'
