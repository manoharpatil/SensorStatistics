import java.io.File

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.io.Source
import scala.util.Try

class SensorStatisticsSpec extends AnyFlatSpec with Matchers {

  "SensorStatistics" should "read CSV files and calculate statistics" in {
    // Set up test data
    val testDir = new File("src/test/resources")
    val csvFiles = testDir.listFiles.filter(_.getName.endsWith(".csv"))
    val expectedNumFiles = csvFiles.length
    val expectedNumMeasurements = csvFiles.flatMap { file =>
      Source.fromFile(file).getLines.drop(1).map { line =>
        line.split(",").map(_.trim) match {
          case Array(sensorId, "NaN") => Measurement(sensorId, None)
          case Array(sensorId, humidityStr) => Try(humidityStr.toInt).toOption.map(humidity => Measurement(sensorId, Some(humidity)))
          case _ => throw new RuntimeException("Invalid CSV format")
        }
      }
    }.length

    val expectedSensorStats = csvFiles.flatMap { file =>
      Source.fromFile(file).getLines.drop(1).flatMap { line =>
        line.split(",").map(_.trim) match {
          case Array(sensorId, "NaN") => Seq(Measurement(sensorId, None))
          case Array(sensorId, humidityStr) => Try(humidityStr.toInt).toOption.map(humidity => Measurement(sensorId, Some(humidity)))
          case _ => Seq.empty
        }
      }
    }.groupBy(_.sensorId).mapValues { measurements =>
      val humidities = measurements.flatMap(_.humidity)
      if (humidities.isEmpty) (None, None, None)
      else (Some(humidities.min), Some(humidities.sum.toDouble / humidities.size), Some(humidities.max))
    }.toSeq.sortBy { case (sensorId, (min, avg, max)) =>
      avg.getOrElse(Double.MinValue)
    }.reverse.map { case (sensorId, (min, avg, max)) =>
      (sensorId, min.getOrElse("NaN"), avg.map("%.0f".format(_)).getOrElse("NaN"), max.getOrElse("NaN"))
    }

    // Run the program
    val outputStream = new java.io.ByteArrayOutputStream()
    Console.withOut(outputStream) {
      SensorStatistics.main(Array(testDir.getPath))
    }
    val output = outputStream.toString

    // Verify the output
    output should include(s"Num of processed files: $expectedNumFiles")
    output should include(s"Num of processed measurements: $expectedNumMeasurements")
    // output should include(s"Num of failed measurements: $expectedNumFailedMeasurements")
    expectedSensorStats.foreach { case (sensorId, min, avg, max) =>
      output should include(s"$sensorId,$min,$avg,$max")
    }
  }

  "SensorStatistics" should "calculate statistics correctly" in {
    val testDir = new File("src/test/resources")
    val measurements: Seq[Measurement] = testDir
      .listFiles
      .filter(_.getName.endsWith(".csv"))
      .flatMap { file =>
        Source.fromFile(file).getLines.drop(1).flatMap { line =>
          line.split(",").map(_.trim) match {
            case Array(sensorId, "NaN") => Seq(Measurement(sensorId, None))
            case Array(sensorId, humidityStr) =>
              Try(humidityStr.toInt).toOption.map(humidity => Measurement(sensorId, Some(humidity)))
            case _ => Seq.empty
          }
        }
      }
      .toSeq

    val numFiles = testDir.listFiles.count(f => f.isFile && f.getName.endsWith(".csv"))
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

    numFiles shouldEqual 2
    numMeasurements shouldEqual 8
    numFailedMeasurements shouldEqual 2

    sortedSensorStats shouldEqual Seq(
      "3" -> (Some(60), Some(60.0), Some(60)),
      "4" -> (Some(55), Some(55.0), Some(55)),
      "1" -> (Some(50), Some(50.0), Some(50)),
      "2" -> (Some(45), Some(45.0), Some(45))
    )
  }

}


