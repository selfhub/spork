import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

val DELIMITER = "#"
val PREFIX = sys.env("AWS_BUCKET_PREFIX")
val SCHEMA_NAME = "fitbit"
val STEP_SUM_INTERVAL = 1000 * 60 * 60 * 24

val FILES = "s3n://" + PREFIX + SCHEMA_NAME + "/*"

val sc = new SparkContext()
val textFile = sc.textFile(FILES).cache()

val OUTPUT_DIR = "temp"
val DESTINATION = "fitbitUserDaySteps.csv"
FileUtil.fullyDelete(new File(OUTPUT_DIR))
FileUtil.fullyDelete(new File(DESTINATION))

// merge sourcePath files into destPath file
def merge(sourcePath: String, destPath: String): Unit =  {
  val hadoopConfig = new Configuration()
  val hdfs = FileSystem.get(hadoopConfig)
  FileUtil.copyMerge(hdfs, new Path(sourcePath),
    hdfs, new Path(destPath), false, hadoopConfig, null)
}

// create key-value pairs where keys are concatenations of user & day,
// and values are the number of steps taken by the user on that day
val userDaySteps = textFile.map(line => {
  val columns = line split ","
  val userDay = columns(0) + DELIMITER + (columns(1).toLong / STEP_SUM_INTERVAL)
  (userDay, columns(2).toLong)
}).reduceByKey((x, y) => {
  x + y
}).map(tuple => {
  val cols = tuple._1 split DELIMITER
  Array(cols(0), cols(1).toLong * STEP_SUM_INTERVAL, tuple._2).mkString(",")
})

userDaySteps.saveAsTextFile(OUTPUT_DIR)
merge(OUTPUT_DIR, DESTINATION)
FileUtil.fullyDelete(new File(OUTPUT_DIR))
FileUtil.fullyDelete(new File("." + DESTINATION + ".crc"))

sys.exit
