package bdhproject.sepsis.ioutils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

/** This is based on the CSVUtils file provided for the homeworks, but modified to use Dataframes */
object CSVUtils {
  def loadCSVAsTable(ss: SparkSession, path: String, tableName: String): DataFrame = {
    val data = ss.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(path)

    data.createOrReplaceTempView(tableName)
    data
  }

  def loadCSVAsTable(ss: SparkSession, path: String): DataFrame = {
    loadCSVAsTable(ss, path, inferTableNameFromPath(path))
  }

  private val pattern = "(\\w+)(\\.csv)?$".r.unanchored
  def inferTableNameFromPath(path: String) = path match {
    case pattern(filename, extension) => filename
    case _ => path
  }
}
