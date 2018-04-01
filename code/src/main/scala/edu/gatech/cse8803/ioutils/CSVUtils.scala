/**
 * @author Hang Su <hangsu@gatech.edu>.
 */
package edu.gatech.cse8803.ioutils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

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
