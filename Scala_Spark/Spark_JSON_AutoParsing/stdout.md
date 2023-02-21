
#1 -----------------------------------

{"lvl1":[{"col1":"BLOCKED","col2":123,"col3":null,"col4":456,"col5":"Text2 (Text3)"},{"col1":"ACTIVE","col2":321,"col3":654,"col4":null,"col5":"Text4 (Text5)"}]}
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties

#2 -----------------------------------

StructType(StructField(lvl1,ArrayType(StructType(StructField(col1,StringType,true), StructField(col2,LongType,true), StructField(col3,LongType,true), StructField(col4,LongType,true), StructField(col5,StringType,true)),true),true))

#3 -----------------------------------

root
|-- js: string (nullable = true)


#4 -----------------------------------

root
|-- js: string (nullable = true)
|-- lvl2: struct (nullable = true)
|    |-- lvl1: array (nullable = true)
|    |    |-- element: struct (containsNull = true)
|    |    |    |-- col1: string (nullable = true)
|    |    |    |-- col2: long (nullable = true)
|    |    |    |-- col3: long (nullable = true)
|    |    |    |-- col4: long (nullable = true)
|    |    |    |-- col5: string (nullable = true)


#5 -----------------------------------

root
|-- lvl3: struct (nullable = true)
|    |-- col1: string (nullable = true)
|    |-- col2: long (nullable = true)
|    |-- col3: long (nullable = true)
|    |-- col4: long (nullable = true)
|    |-- col5: string (nullable = true)


#6 -----------------------------------

root
|-- col1: string (nullable = true)
|-- col2: long (nullable = true)
|-- col3: long (nullable = true)
|-- col4: long (nullable = true)
|-- col5: string (nullable = true)


#7 -----------------------------------


Process finished with exit code 0