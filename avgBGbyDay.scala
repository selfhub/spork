import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

val DELIMITER = "#"
val SCHEMA_PREFIX = "selfhub-io-schema-dduugg-" // sys.env("AWS_BUCKET_PREFIX")
val SCHEMA_NAME = "dexcom"
val FILES = "s3n://" + SCHEMA_PREFIX + SCHEMA_NAME + "/*"
val AVG_OVER_INTERVAL: Long = 1000 * 60 * 60 * 24
val RESULTS_BUCKET = "selfhub-io-results-dduugg"
val RESULTS_FOLDER = "dexcomAvgBGUserDay"

val timeStart = Calendar.getInstance.getTimeInMillis

//val sc = new SparkContext()
val textFile = sc.textFile(FILES)

// create CSV entries, where the first column is the timestamp and the second
// column is the average BG value over that interval
val avgBGByDay = textFile.map(line => {
  val columns = line.split(",")
  val userDay = columns(0) + DELIMITER + (columns(1).toLong / AVG_OVER_INTERVAL)
  (userDay, columns(2).toInt)
}).combineByKey((v) => (v, 1),
    (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
    (acc1: (Int, Int), acc2: (Int, Int)) =>
      (acc1._1 + acc2._1, acc1._2 + acc2._2)
).map{ case (key, value) => {
  val userDayArray = key.split(DELIMITER)
  val epochDay = (userDayArray(1).toLong * AVG_OVER_INTERVAL)
  Array(userDayArray(0), epochDay, value._1 / value._2.toFloat).mkString(",")
}}

avgBGByDay.saveAsTextFile("s3n://" + RESULTS_BUCKET + "/" + RESULTS_FOLDER)

val elapsedMillis = Calendar.getInstance.getTimeInMillis - timeStart
val elapsedMins = TimeUnit.MINUTES.convert(elapsedMillis, TimeUnit.MILLISECONDS)

println("Took " + elapsedMins + " minutes")

// sys.exit
