package scala.handler

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties
import scala.io.{BufferedSource, Source}
import scala.model.Trip


class TripsHandlerScala()  extends Serializable {
  @transient val sparkConf: SparkConf = new SparkConf().setAppName("homework_scala").setMaster("local[*]")
  @transient val sc = new SparkContext(sparkConf)
  sc.setLogLevel("ERROR")
  val x = new Properties
  val propertiesFile: String = "src/main/resources/application.properties"
  val properties: Properties = new Properties()
  @transient  val source: BufferedSource = Source.fromFile(propertiesFile)
  properties.load(source.bufferedReader())
  val driversFileName: String = properties.getProperty("input_file_drivers")
  val tripsFileName: String = properties.getProperty("input_file_trips")
  val cityNameToCheck: String = properties.getProperty("city_name_to_check")
  val distance: String = properties.getProperty("min_distance")
  println(tripsFileName)
  val lines: RDD[String] = readFile(tripsFileName);

  def readFile(fileName: String): RDD[String] = {
    sc.textFile(fileName)
  }

  def countNumberOfLines(): Unit = {
      println("Number of lines in files " + tripsFileName + " is " + lines.count())
  }

  def filterOutCityToCheck(): RDD[Trip] = lines.map((line: String) => {
    val words = line.split(" ")
    Trip(words(0).trim, words(1).trim, words(2).trim.toInt)
  }).filter((trip:Trip) => trip.tripCity.equalsIgnoreCase(cityNameToCheck)).persist(StorageLevel.MEMORY_AND_DISK)

  def calculateNumberOfTripsLongerThanXKm(persist: RDD[Trip]): Unit = {
     val totalDistance = persist.filter(trip=> trip.tripDistance > distance.toInt).count
    println("Number of trips to " + cityNameToCheck + " longer than " + distance + " km is: " + totalDistance);
  }

  def calculateTotalTripsDistanceToSpecificCity(persist: RDD[Trip]): Unit = {
    println("Total killomitrage of trips to " + cityNameToCheck + " is: " + persist.map(trip=>trip.tripDistance).sum())
  }

  def findBestDrivers(): Unit = {
    val bestDriversNames = readFile(driversFileName).map((line: String) => {
      val words = line.split(", ")
      new Tuple2[String, String](words(0), words(1))
    })

    println( lines.map((line: String) => {
      val words = line.split(" ")
      Tuple2[String, Integer](words(0), words(2).toInt)
    }
    ).reduceByKey(_ + _).join(bestDriversNames).sortBy(tuple => tuple._2, false).take(3).mkString("(", ", ", ")"));
  }


}
