import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

val DELIMITER = "#"
val SCHEMA_PREFIX = "selfhub-io-schema-dduugg-" // sys.env("AWS_BUCKET_PREFIX")
val SCHEMA_NAME = "fitbit"
val FILES = "s3n://" + SCHEMA_PREFIX + SCHEMA_NAME + "/*"
val STEP_SUM_INTERVAL = 1000 * 60 * 60 * 24
val RESULTS_BUCKET = "selfhub-io-results-dduugg"
val RESULTS_FOLDER = "fitbitUserDayStepCounts"

val timeStart = Calendar.getInstance.getTimeInMillis

val sc = new SparkContext()
val textFile = sc.textFile(FILES)

// create key-value pairs where keys are concatenations of user & day,
// and values are the number of steps taken by the user on that day
val userDaySteps = textFile.map(line => {
  val columns = line.split(",")
  val userDay = columns(0) + DELIMITER + (columns(1).toLong / STEP_SUM_INTERVAL)
  (userDay, columns(2).toLong)
}).reduceByKey((x, y) => {
  x + y
}).map(tuple => {
  val cols = tuple._1 split DELIMITER
  Array(cols(0), cols(1).toLong * STEP_SUM_INTERVAL, tuple._2).mkString(",")
})

userDaySteps.saveAsTextFile("s3n://" + RESULTS_BUCKET + "/" + RESULTS_FOLDER)

val elapsedMillis = Calendar.getInstance.getTimeInMillis - timeStart
val elapsedMins = TimeUnit.MINUTES.convert(elapsedMillis, TimeUnit.MILLISECONDS)

println("Took " + elapsedMins + " minutes")

//sys.exit
