# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading CSV file

# COMMAND ----------

# MAGIC %md
# MAGIC ###### To see what volumes are present in workspace

# COMMAND ----------

# MAGIC %sql
# MAGIC show volumes in  workspace.default

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### To get path where data is stored
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls('/Volumes/workspace/default/csv_files/'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### To read data from  path

# COMMAND ----------

from pyspark.sql.functions import *
df= spark.read.format("csv").option("Inferschema","True").option("header","True").load("/Volumes/workspace/default/csv_files/BigMart Sales.csv")
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Reading from Json file

# COMMAND ----------

# MAGIC %%sql
# MAGIC show volumes from workspace.default

# COMMAND ----------

# MAGIC %md
# MAGIC ###### To see the path where data is present

# COMMAND ----------

dbutils.fs.ls("/Volumes/workspace/default/json_files/")

# COMMAND ----------

df_json=spark.read.format("json").option("header","true")\
.option("inferschema","true")\
    .option("multiline","False").load("/Volumes/workspace/default/json_files/drivers.json")
df_json.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### DDL Schema [To change schema i.e structure of data column names + data types]

# COMMAND ----------

my_ddl_schema =''' Item_Identifier STRING ,
Item_Weight STRING ,
Item_Fat_Content STRING ,
Item_Visibility double ,
Item_Type STRING ,
Item_MRP double ,
Outlet_Identifier STRING ,
Outlet_Establishment_Year integer ,
Outlet_Size STRING ,
Outlet_Location_Type STRING ,
Outlet_Type STRING ,
Item_Outlet_Sales double '''


# COMMAND ----------

df=spark.read.format('csv').option('header','true').schema(my_ddl_schema).load('/Volumes/workspace/default/csv_files/BigMart Sales.csv')
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ####structtype()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

my_struct_schema=StructType([
StructField('Item_Identifier',StringType(),True),
StructField('Item_Weight',StringType(),True),
StructField('Item_Fat_Content',StringType() ,True),
StructField('Item_Visibility',StringType() ,True),
StructField('Item_Type',StringType() ,True),
StructField('Item_MRP',StringType() ,True),
StructField('Outlet_Identifier',StringType() ,True),
StructField('Outlet_Establishment_Year',IntegerType() ,True),
StructField('Outlet_Size',StringType() ,True),
StructField('Outlet_Location_Type',StringType() ,True),
StructField('Outlet_Type',StringType() ,True),
StructField('Item_Outlet_Sales',StringType(),True)
])

# COMMAND ----------

df=spark.read.format('csv').option('header','true').schema(my_struct_schema).load('/Volumes/workspace/default/csv_files/BigMart Sales.csv')
df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Select

# COMMAND ----------

df.select(col('Item_Identifier'),col('Item_Type'),col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Alias

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_Id')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Filter

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario - 1  [Item_Fat_Content ==Regular ]

# COMMAND ----------

from pyspark.sql.functions import *

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scenario -2 [Item_Type='soft Drinks and Item_weight<10]

# COMMAND ----------

df.filter((col('Item_Type') == 'Soft Drinks') & (col('Item_Weight')<10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario -3 [Outlet_Location_Type (Tier1,Tier2) and Outlet_Size is null]

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Withcolumnrename

# COMMAND ----------

df.withColumnRenamed("Item_Weight","Item_WT").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Withcolumn

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 1 [adding new column with constant value]

# COMMAND ----------

from pyspark.sql.functions import *
df= spark.read.format("csv").option("Inferschema","True").option("header","True").load("/Volumes/workspace/default/csv_files/BigMart Sales.csv")
df.display()
df=df.withColumn("Flag",lit("new"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario -2 [adding new column with transformation]

# COMMAND ----------

df=df.withColumn("multiply",col("Item_Weight")*col("Item_MRP"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####scenario -3 [modifying existing column]

# COMMAND ----------

from pyspark.sql.functions import *

df.withColumn("Item_Fat_Content", regexp_replace(col("Item_Fat_Content"), "Regular", "Reg"))\
    .withColumn("Item_Fat_Content", regexp_replace(col("Item_Fat_Content"), "Low Fat", "LF")).display()



# COMMAND ----------

# MAGIC %md
# MAGIC ###Typecasting

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
df= spark.read.format("csv").option("Inferschema","True").option("header","True").load("/Volumes/workspace/default/csv_files/BigMart Sales.csv")
df.display()

df=df.withColumn('Item_Visibility',col('Item_Visibility').cast(StringType()))
df.display()

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #####sort/order by

# COMMAND ----------

# MAGIC %md
# MAGIC #####Scenario -1 [Item_weight by desc]

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

df= spark.read.format("csv").option("Inferschema","True").option("header","True").load("/Volumes/workspace/default/csv_files/BigMart Sales.csv")
df.display()

df=df.sort(col('Item_Weight').desc())
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario -2 [Item_Visibility by asc]

# COMMAND ----------

df=df.sort(col('Item_Visibility').asc())

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Scenario -3 [Item_Visibility by asc and Item_Weight by desc] and using limit to get 20 records

# COMMAND ----------

df=df.sort(['Item_Visibility','Item_Weight'],ascending=[1,0]).limit(20)
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Drop

# COMMAND ----------

df.drop("Item_Visibility").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Scenario -2 [drop mutlitple columns]
# MAGIC

# COMMAND ----------

df.drop("Item_Weight","Item_MRP").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Drop_Duplicates / dedup

# COMMAND ----------

# MAGIC %md
# MAGIC #######Senario -1 [ drop duplicates of all the columns]

# COMMAND ----------

df.dropDuplicates().display()
df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Scenario -2 [drop duplicates of a column]

# COMMAND ----------

df.dropDuplicates(subset=['Item_Weight','Item_Fat_Content']).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Union and UnionbyName

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

data1=[('1','Sai'),
       ('2','Ram')]

schema1=['Id','name']

df1=spark.createDataFrame(data1,schema1)

#df1.display()


data2=[('3','Sri'),
       ('4','shiva')]

schema2=''' Id string,
            name string'''

df2=spark.createDataFrame(data2,schema2)
#df2.display()
union_data=df1.union(df2)
union_data.display()

data3=[('Elena','5'),
       ('Stefan','6')]

structschema=StructType([
    StructField('name',StringType(),True),
    StructField('Id', StringType(),False)
   
])

df3=spark.createDataFrame(data3,structschema)
df3.display()
#df1.union(df3).display() #here the data is messing when col are not in correct order

union_by_name=df1.unionByName(df3) # so we use unionBYName



# COMMAND ----------

# MAGIC %md
# MAGIC ####String Functions [Initcap,Upper,Lower]

# COMMAND ----------



df=df.withColumn('Item_Type_Title',initcap(col('Item_Type')))
df=df.withColumn('lower_item_type_title',lower(col('Item_Type')))
df=df.withColumn('upper_imte_type_title',upper(col('Item_Type')))


# COMMAND ----------

# MAGIC %md
# MAGIC ####Date  functions

# COMMAND ----------

df=df.withColumn('current_date',current_date())
df=df.withColumn('week_after',date_add('current_date',7))
df=df.withColumn('week_before',date_sub('current_date',7))
df=df.withColumn('datediff',date_diff('week_after','current_date'))
df=df.withColumn('week_before',date_format('week_before','dd-MM-yyyy'))
df=df.withColumn('datediff1',date_diff('current_date',to_date('week_before','dd-MM-yyyy')))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Handling Nulls

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

df= spark.read.format("csv").option("Inferschema","True").option("header","True").load("/Volumes/workspace/default/csv_files/BigMart Sales.csv")

df.display()

df.dropna('all').display() # drops records with  null values in all columns
df.dropna('any').display() # drops records with any null values in columns
df.dropna('any',subset=['Outlet_Size']).display() # drops records with any null values in Outlet_Size column
df.dropna('all',subset=['Outlet_Size']).display() # drops records with null values in Outlet_Size column


# COMMAND ----------

# MAGIC %md
# MAGIC ###Fill null values

# COMMAND ----------

df.fillna('Not Available',subset=['Outlet_size']).display()
df=df.fillna({'Outlet_size':'Not Available','Item_Weight':0})
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Split and Indexing 

# COMMAND ----------

df=df.withColumn('outlet_Type_split_index1',split('outlet_Type',' ')[1])

# COMMAND ----------

# MAGIC %md
# MAGIC ####Explode [elements in List explode into rows]

# COMMAND ----------

df_exp=df.withColumn('outlet_Type',split('Outlet_Type',' '))
df_exp.display()

df_exp=df_exp.withColumn('outlet_Type',explode('outlet_Type'))


# COMMAND ----------

# MAGIC %md
# MAGIC ####Array contains

# COMMAND ----------

df_exp.withColumn('Type1_flag',array_contains('outlet_Type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Group by

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

df= spark.read.format("csv").option("Inferschema","True").option("header","True").load("/Volumes/workspace/default/csv_files/BigMart Sales.csv")
df.display()

df_groupby=df.groupBy('Item_Type').agg(sum('Item_MRP').alias('sum_item_mrp'))
df_groupby=df.groupBy('ITem_Type').agg(avg("Item_MRP")).display()
df_groupby=df.groupBy('Item_Type').agg(avg('Item_MRP').alias('avg_item_mrp'))
df_groupby=df.groupby("Item_Type","Outlet_Size").agg(sum("Item_Visibility"),avg("Item_MRP")).display()
df_groupby.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Collect_list [collects values into an array per group ]

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

collect_data=[
    ('user1','book1'),
    ('user1','book2'),
    ('user2','book3'),
    ('user3','book4'),
    ('user3','book5'),
    ('user2','book6')
]
collect_schema='User String,Book  String'
df_book=spark.createDataFrame(collect_data,schema=collect_schema)
#df_book.display()
df_book=df_book.groupBy('User').agg(collect_list('Book').alias('Books'))
df_book.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Pivot

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

#df= spark.read.format("csv").option("Inferschema","True").option("header","True").load("/Volumes/workspace/default/csv_files/BigMart Sales.csv")
#df.display()

df_pivot=df.select('Item_Type','Item_MRP','Outlet_Size')


df_pivot=df_pivot.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).orderBy(['Item_Type','High'],ascending=[1,0])
df_pivot.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####when otherwise

# COMMAND ----------

# MAGIC %md
# MAGIC #####Scenario -1 with one condition

# COMMAND ----------

df=df.withColumn('Item_Type_Flag',when(col('Item_Type')=='Meat','Non-Veg').otherwise('Veg'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Scenario -2 [with multiple conditions]

# COMMAND ----------

df=df.withColumn('Item_Veg_Type_Expensive',when(((col('Item_Type_Flag')=='Veg') & (col('Item_MRP')>100)),'Veg_Expensive')\
.when(((col('Item_Type_Flag')=='Veg') & (col('Item_MRP')<100)),'Veg_InExpensive')\
                                        .otherwise('Non-Veg'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Joins

# COMMAND ----------

emp_data=[
    (1,'Gaur','d01'),
    (2,'kit','d02'),
    (3,'sam','d03'),
    (4,'tim','d03'),
    (5,'aman','d05'),
    (6,'nad','d06')
]
emp_schema='emp_id int,emp_name string,dept_id string'
df_emp=spark.createDataFrame(emp_data,emp_schema)
df_emp.display()

dept_data=[
    ('d01','HR'),
    ('d02','Marketing'),
    ('d03','Accounts'),
    ('d04','IT'),
    ('d05','Finance')
]

dept_schema='dept_id string, dept_name string'
df_dept=spark.createDataFrame(dept_data,dept_schema)
df_dept.display()

df_emp.join(df_dept,df_emp['dept_id']==df_dept['dept_id'],'inner').display()  #Inner join
df_emp.join(df_dept,df_emp['dept_id']==df_dept['dept_id'],'left').display() #left join
df_emp.join(df_dept,df_emp['dept_id']==df_dept['dept_id'],'right').display() #right join
df_emp.join(df_dept,df_emp['dept_id']==df_dept['dept_id'],'outer').display() #outer join
df_emp.join(df_dept,df_emp['dept_id']==df_dept['dept_id'],'anti').display() #anti join


# COMMAND ----------

# MAGIC %md
# MAGIC #####Window Functions[Performs calucaltion across rows that are related to current row without collapsing rows]

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *


df=df.withColumn('row_no',row_number().over(Window.orderBy(col('Item_Identifier')))) \
.withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier').desc()) ))\
    .withColumn('dense_rank',dense_rank().over(Window.orderBy(col('Item_Identifier') ))) #dense_rank
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ####cumulative sum

# COMMAND ----------

df.withColumn('cum_sum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

df.withColumn('total_sum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###User Defined Functions 
# MAGIC ########[Mostly avoid it because in takes time for the executor i.e in Java Virtual Machine every time to convert to python interpreter when we write code in python and their is loss of performance ]

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step -1 [create your function in python]

# COMMAND ----------

def square(x):
  return x*x

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step -2 [convert the function into pyspark function]
# MAGIC

# COMMAND ----------

py_square=udf(square)
df=df.withColumn('Square_of_MRP',py_square('Item_MRP')).orderBy('Item_Type')
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Writing

# COMMAND ----------

# MAGIC %md
# MAGIC ###Append ,Overwrite,Error,Ignore

# COMMAND ----------

from datetime import datetime

today=datetime.today().strftime('%Y-%m-%d')

df.write.format('csv').mode('append').option('header','true').option('path',f"/Volumes/workspace/default/output/csv_files/BigMart Sales_{today}").save() # Append
df.write.format('csv').mode('overwrite').option('header','true').option('path',f"/Volumes/workspace/default/output/csv_files/BigMart Sales_{today}").save() # Overwrite
df.write.format('csv').option('header','true').mode('ignore').option('path',f"/Volumes/workspace/default/output/csv_files/BigMart Sales_{today}").save() # Ignore
df.write.format('csv').option('header','true').mode('error').option('path',f"/Volumes/workspace/default/output/csv_files/BigMart Sales_{today}").save() # Error


# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/output/csv_files/"))

# COMMAND ----------

from datetime import datetime

today=datetime.today().strftime('%Y-%m-%d')
df_read=spark.read.format('csv').option('header','true').option('inferschema','true').load("/Volumes/workspace/default/output/csv_files/BigMart Sales_2026-01-03")

df_read.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Parquet

# COMMAND ----------


df_read.write.format('parquet').mode('overwrite').option('path',f"/Volumes/workspace/default/output/parquet_files/BigMart Sales_2026-01-03/").save()



# COMMAND ----------

# MAGIC %md
# MAGIC ####Table

# COMMAND ----------

df_read.write.format("delta").option('header','true') \
   .mode("overwrite") \
   .saveAsTable("BigMart")

# COMMAND ----------

# MAGIC %%sql
# MAGIC select * from BigMart order by row_no asc

# COMMAND ----------

# MAGIC %md
# MAGIC ####Spark Sql

# COMMAND ----------

# MAGIC %md
# MAGIC #####create tempview 

# COMMAND ----------

df_read.createTempView("my_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_view where Item_Type='Canned' and Item_Fat_Content='Regular'

# COMMAND ----------

df_sql=spark.sql("select * from my_view where Item_Type='Canned' and Item_Fat_Content='Regular'")  # store data back to df
df_sql.display()


# COMMAND ----------

# MAGIC %md
# MAGIC