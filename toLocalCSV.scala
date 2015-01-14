import java.io.File
import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileUtil, FileSystem}
import org.apache.spark.SparkContext

val DESTINATION = "fitbitUserDaySteps.csv"
val RESULTS_BUCKET = "selfhub-io-results-dduugg"
val RESULTS_FOLDER = "fitbitUserDayStepCounts"
val TEMP_LOCATION = "temp"

// merge sourcePath files into destPath file
def merge(sourcePath: String, destPath: String): Unit =  {
  val hadoopConfig = new Configuration()()
  val hdfs = FileSystem.get(hadoopConfig)
  FileUtil.copyMerge(hdfs, new Path(sourcePath),
      hdfs, new Path(destPath), false, hadoopConfig, null)
}

val source = "s3n://" + RESULTS_BUCKET + "/" + RESULTS_FOLDER

val timeStart = Calendar.getInstance.getTimeInMillis

//val sc = new SparkContext()
val textFile = sc.textFile(source)
textFile.saveAsTextFile(TEMP_LOCATION)
merge(TEMP_LOCATION, DESTINATION)

FileUtil.fullyDelete(new File(TEMP_LOCATION))
FileUtil.fullyDelete(new File("." + DESTINATION + ".crc"))

val elapsedMillis = Calendar.getInstance.getTimeInMillis - timeStart
val elapsedMins = TimeUnit.MINUTES.convert(elapsedMillis, TimeUnit.MILLISECONDS)

println("Took " + elapsedMins + " minutes")

// sys.exit
