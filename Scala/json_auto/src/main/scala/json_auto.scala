import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.functions._

object json_auto {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    println("\n#1 -----------------------------------\n")
    // Создаем пример JSON строки
    val t = """{"lvl1":[{"col1":"BLOCKED","col2":123,"col3":null,"col4":456,"col5":"Text2 (Text3)"},{"col1":"ACTIVE","col2":321,"col3":654,"col4":null,"col5":"Text4 (Text5)"}]}"""

    println(t)

    val spark = SparkSession
      .builder()
      .config("spark.master", "local[*]")
      .appName("NullHandling")
      .getOrCreate()

    import spark.implicits._

    println("\n#2 -----------------------------------\n")
    // Преобразуем JSON строку в RDD объект
    // и читаем его как JSON
    // получаем автоматом схему JSON
    val rawData = List(t)
    val params_rdd = spark.sparkContext.parallelize(rawData)
    val schema = spark.read.json(params_rdd).schema
    println( schema )

    println("\n#3 -----------------------------------\n")

    // Создаем объект Spark DataFrame
    // внутри которого в ячейке JSON
    val sdf1 = Seq(t).toDF("js")
    sdf1.printSchema()

    println("\n#4 -----------------------------------\n")

    // Парсим ячейку JSON при помощи полученной схемы
    val sdf2 = sdf1
      .withColumn("lvl2", from_json( col("js"), schema))

    sdf2.printSchema()

    println("\n#5 -----------------------------------\n")

    val sdf3 = sdf2
      .select( explode(col("lvl2.lvl1")).as("lvl3") )

    sdf3.printSchema()

    println("\n#6 -----------------------------------\n")

    val sdf_result = sdf3.select("lvl3.*")

    sdf_result.printSchema()

    println("\n#7 -----------------------------------\n")
  }
}