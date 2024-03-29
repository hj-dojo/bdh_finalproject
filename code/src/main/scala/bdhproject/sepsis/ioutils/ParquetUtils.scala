package bdhproject.sepsis.ioutils


import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.rdd.RDD
import bdhproject.sepsis.model.{ChartEvents, ICUStay, Prescriptions, MicrobiologyEvents}

object ParquetUtils {
  def loadParquetAsDataFrame(ss: SparkSession, path: String): DataFrame = {
    import ss.implicits._
    val data = ss.read.parquet(path)
    data
  }

  def saveDataFrameAsParquet(ss: SparkSession, dataDF: DataFrame, path: String) = {
    import ss.implicits._
    dataDF.coalesce(1).write.mode(SaveMode.Overwrite).parquet(path)
  }

}
