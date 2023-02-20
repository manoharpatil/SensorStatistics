import java.io.File
import scala.io.Source
import scala.util.Try

case class Measurement(sensorId: String, humidity: Option[Int])

object SensorStatistics extends App {
  // Check that the directory path is provided as a command-line argument
  if (args.length != 1) {
    println("Please provide the path to a directory with CSV files as a command-line argument.")
    System.exit(1)
  }

  // Read all CSV files in the directory and parse their contents into a sequence of measurements
  val measurements: Seq[Measurement] = new File(args(0)).listFiles.filter(_.getName.endsWith(".csv")).flatMap { file =>
    Source.fromFile(file).getLines.drop(1).flatMap { line =>
      line.split(",").map(_.trim) match {
        case Array(sensorId, "NaN") => Seq(Measurement(sensorId, None))
        case Array(sensorId, humidityStr) => Try(humidityStr.toInt).toOption.map(humidity => Measurement(sensorId, Some(humidity)))
        case _ => Seq.empty
      }
    }
  }.toSeq

  // Calculate the required statistics
  val numFiles = new File(args(0)).listFiles.count(_.getName.endsWith(".csv"))
  val numMeasurements = measurements.size
  val numFailedMeasurements = measurements.count(_.humidity.isEmpty)
  val sensorStats = measurements.groupBy(_.sensorId).mapValues { measurements =>
    val humidities = measurements.flatMap(_.humidity)
    if (humidities.isEmpty) (None, None, None)
    else (Some(humidities.min), Some(humidities.sum.toDouble / humidities.size), Some(humidities.max))
  }
  val sortedSensorStats = sensorStats.toSeq.sortBy { case (sensorId, (min, avg, max)) =>
    avg.getOrElse(Double.MinValue)
  }.reverse

  // Print the output
  println(s"Num of processed files: $numFiles")
  println(s"Num of processed measurements: $numMeasurements")
  println(s"Num of failed measurements: $numFailedMeasurements")
  println("\nSensors with highest avg humidity:")
  println("sensor-id,min,avg,max")
  sortedSensorStats.foreach { case (sensorId, (min, avg, max)) =>
    println(s"$sensorId,${min.getOrElse("NaN")},${avg.map("%.0f".format(_)).getOrElse("NaN")},${max.getOrElse("NaN")}")
  }
}
