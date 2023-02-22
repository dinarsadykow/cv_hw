import scala.util.Try
import scala.math.max
import org.apache.spark._
import org.apache.log4j._

import org.apache.spark.rdd.RDD

object rdd_view {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "spark_local")

    println("\n---------------------------------\n")

    val cities: Vector[String] = Vector("Moscow", "Paris", "Madrid", "London", "New York")
    val rdd1 = sc.parallelize(cities)
    println(s"The RDD has ${rdd1.count()} elemtnts, the first one is ${rdd1.first()}")

    println("\n---------------------------------\n")

    val rdd = sc.textFile("airport-codes.csv")

    println(rdd.getNumPartitions)

    println("\n---------------------------------\n")

    val filtered = rdd.filter(city => city.startsWith("M"))

    filtered.take(4).foreach(println)

    println("\n---------------------------------\n")

    val upperRdd = rdd.map(city => city.toUpperCase())
    upperRdd.take(4).foreach(println)

    println("\n---------------------------------\n")

    val twoElementsSorted: Array[String] = rdd.takeOrdered(2)(Ordering[String].reverse)


    println("\n---------------------------------\n")

    val mappedRdd: RDD[Vector[Char]] = rdd.map(x => x.toVector)
    mappedRdd.take(4).foreach(println)

    println("\n---------------------------------\n")

    val flatMappedRdd = rdd.flatMap(x => x.toLowerCase)
    flatMappedRdd.take(4).foreach(println)

    println("\n---------------------------------\n")

    val pairRdd: RDD[(Char, Int)] = rdd.flatMap(x => x.toLowerCase).map(x => (x, 1))
    pairRdd.take(4).foreach(println)

    println("\n---------------------------------\n")

    val letterCount1: scala.collection.Map[Char, Long] = pairRdd.countByKey()

    letterCount1.take(4).foreach(println)

    println("\n---------------------------------\n")

    println(letterCount1.take(4).mkString(" "))

    println("\n---------------------------------\n")

    val letterCount: RDD[(Char, Int)] = pairRdd.reduceByKey({ (x, y) => x + y })

    println(letterCount.take(4).mkString(" "))

    println("\n---------------------------------\n")

    // Word count
    val wordCount = rdd
      .flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey((x, y) => x + y)
      .top(10)(Ordering[Int].on(_._2))

    wordCount.take(4).foreach(println)

    println("\n---------------------------------\n")

    val favouriteLetters: Vector[Char] = Vector('a', 'd', 'o')
    val favLetRdd: RDD[(Char, Int)] = sc.parallelize(favouriteLetters).map(x => (x, 1))
    favLetRdd.take(4).foreach(println)

    println("\n---------------------------------\n")


    val joined: RDD[(Char, (Int, Option[Int]))] = letterCount.leftOuterJoin(favLetRdd)
    joined.take(4).foreach(println)


    println("\n---------------------------------\n")

    joined.collect().foreach {
      j =>
        // Разбиваем элемент j на три элемента: letter, leftCount, rightCount
        val (letter: Char, (leftCount: Int, rightCount: Option[Int])) = j

        rightCount match {
          case Some(v) => println(s"1. $letter - $leftCount ")
          case None => println(s"2. $letter ")
        }
    }

    println("\n---------------------------------\n")

    val firstElem = rdd.first
    val brodcastFirstElem = sc.broadcast(firstElem)
    val noHeader = rdd
      .filter(x => x != brodcastFirstElem.value)
      .map(x => x.replaceAll("\"", ""))

    noHeader.take(4).foreach(println)

    println("\n---------------------------------\n")

    case class Airport(
                        ident: String,
                        type1: String,
                        name: String,
                        elevationFt: Option[Int],
                        continent: String,
                        isoCountry: String,
                        isoRegion: String,
                        municipality: String,
                        gpsCode: String,
                        iataCode: String,
                        localCode: String,
                        longitude: Option[Float],
                        latitude: Option[Float]
                      )

    def toAirportOpt(data: String): Option[Airport] = {
      val airportArr: Array[String] = data.split(",", -1)

      airportArr match {
        case Array(
        ident,
        aType,
        name,
        elevationFt,
        continent,
        isoCountry,
        isoRegion,
        municipality,
        gpsCode,
        iataCode,
        localCode,
        longitude,
        latitude) => {

          Some(
            Airport(
              ident = ident,
              type1 = aType,
              name = name,
              elevationFt = Try(elevationFt.toInt).toOption,
              continent = continent,
              isoCountry = isoCountry,
              isoRegion = isoRegion,
              municipality = municipality,
              gpsCode = gpsCode,
              iataCode = iataCode,
              localCode = localCode,
              longitude = Try(longitude.toFloat).toOption,
              latitude = Try(latitude.toFloat).toOption
            )
          )
        }
        case _ => Option.empty[Airport]
      }

    }

    println("\n---------------------------------\n")

    val airportRdd: RDD[Airport] = noHeader.flatMap(x => toAirportOpt(x))
    airportRdd.take(5).foreach(println)
    println(airportRdd.count)

    println("\n---------------------------------\n")

    val pairAirport = airportRdd.map(x => (x.isoCountry, x.elevationFt))
    pairAirport.take(5).foreach(println)

    println("\n---------------------------------\n")

    val fixedElevation = pairAirport.map {
      case (k, Some(v)) => (k, v)
      case (k, None) => (k, Int.MinValue)
    }
    fixedElevation
      .filter(x => x._2 == Int.MinValue)
      .take(5).foreach(println)

    println("\n---------------------------------\n")

    val result = fixedElevation
      .reduceByKey { (x, y) => Math.max(x, y) }
      .collect
      .sortBy { case (_, height) => -height }

    result.take(10).foreach(println)

    println("\n---------------------------------\n")

  }

}
