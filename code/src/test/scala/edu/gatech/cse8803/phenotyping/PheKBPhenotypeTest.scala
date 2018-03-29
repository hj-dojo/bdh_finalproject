package edu.gatech.cse8803.phenotyping

import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfter
import org.scalatest.Matchers
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import edu.gatech.cse8803.main.Main
import edu.gatech.cse8803.phenotyping

class PheKBPhenotypeTest extends FlatSpec with BeforeAndAfter with Matchers {
//  var ss: SparkSession = _
//
//  before {
//    Logger.getRootLogger().setLevel(Level.WARN)
//    Logger.getLogger("org").setLevel(Level.WARN)
//    ss = SparkSession
//      .builder.master("local")
//      .appName("Test PheKBPhenotype")
//      .getOrCreate()
//  }
//
//  after {
//    ss.stop()
//  }
//
//  "transform" should "give expected results" in {
//    val (antibiotics, sepsis_codes, vitals_itemids) = Main.loadStaticRawData(ss)
//    val (med, lab, diag) = Main.loadRddRawData(ss, antibiotics, sepsis_codes, vitals_itemids)
//    val rdd = phenotyping.T2dmPhenotype.transform(med, lab, diag)
//    val cases = rdd.filter{case (x, t) => t == 1}.map{case (x, t) => x}.collect.toSet
//    val controls = rdd.filter{case (x, t) => t == 2}.map{case (x, t) => x}.collect.toSet
//    val others = rdd.filter{case (x, t) => t == 3}.map{case (x, t) => x}.collect.toSet
//    cases.size should be (427 + 255 + 294)
//    controls.size should be (948)
//    others.size should be (3688 - cases.size - controls.size)
//  }
}
