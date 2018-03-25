/**
 * @author Hang Su
 */
package edu.gatech.cse8803.features

import edu.gatech.cse8803.model.{Diagnostic, LabResult, Medication}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.collection.mutable.ListBuffer


object FeatureConstruction {

  /**
   * ((patient-id, feature-name), feature-value)
   */
  type FeatureTuple = ((String, String), Double)

  /**
   * Aggregate feature tuples from diagnostic with COUNT aggregation,
   * @param diagnostic RDD of diagnostic
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic]): RDD[FeatureTuple] = {

    /* Group on patient id and diagnosis code and count occurances */
    val diag_features = diagnostic.map(diag => ((diag.patientID, diag.code), 1.0)).reduceByKey(_ + _)

//    println("diag_features : " + diag_features.count())
    diag_features
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation,
   * @param medication RDD of medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication]): RDD[FeatureTuple] = {
    /* Group on patient id and medicine and count occurances */
    val med_features = medication.map(med => ((med.patientID, med.medicine), 1.0)).reduceByKey(_ + _)

//    println("med_features : " + med_features.count())
    med_features
  }

  /**
   * Aggregate feature tuples from lab result, using AVERAGE aggregation
   * @param labResult RDD of lab result
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult]): RDD[FeatureTuple] = {
    /* Group on patient id and test name and compute average of test results */
    val lab_features = labResult.map(lab => ((lab.patientID, lab.testName), lab.value))
                                .aggregateByKey((0.0,0.0))((acc, value) =>
                                    (acc._1 + value, acc._2 + 1.0),
                                    (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
                                .mapValues(x => (x._1/x._2))
//    println("lab_features : " + lab_features.count())
    lab_features
  }

  /**
   * Aggregate feature tuple from diagnostics with COUNT aggregation, but use code that is
   * available in the given set only and drop all others.
   * @param diagnostic RDD of diagnostics
   * @param candiateCode set of candidate code, filter diagnostics based on this set
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic], candiateCode: Set[String]): RDD[FeatureTuple] = {
    /* Group on patient id and diagnosis code, Filter for relevant codes only, then count occurances */
    val diag_features = diagnostic.map(diag => ((diag.patientID, diag.code), 1.0))
                                  .filter{case (k,v) => candiateCode.contains(k._2)}
                                  .reduceByKey(_ + _)

//    println("filtered diag_features : " + diag_features.count())
    diag_features
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation, use medications from
   * given set only and drop all others.
   * @param medication RDD of diagnostics
   * @param candidateMedication set of candidate medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication], candidateMedication: Set[String]): RDD[FeatureTuple] = {
    /* Group on patient id and medication, Filter for relevant medicines only, then count occurances */
    val med_features = medication.map(med => ((med.patientID, med.medicine), 1.0))
                                 .filter{case (k,v) => candidateMedication.contains(k._2)}
                                 .reduceByKey(_ + _)
//    println("filtered med_features : " + med_features.count())
    med_features
  }


  /**
   * Aggregate feature tuples from lab result with AVERAGE aggregation, use lab from
   * given set of lab test names only and drop all others.
   * @param labResult RDD of lab result
   * @param candidateLab set of candidate lab test name
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult], candidateLab: Set[String]): RDD[FeatureTuple] = {
    /* Group on patient id and test name, filter for relevant labs only, then compute average of test results */
    val lab_features = labResult.map(lab => ((lab.patientID, lab.testName), lab.value))
                                .filter{case (k,v) => candidateLab.contains(k._2)}
                                .aggregateByKey((0.0,0.0))((acc, value) =>
                                  (acc._1 + value, acc._2 + 1.0),
                                  (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
                                .mapValues(x => (x._1/x._2))
//    println("filtered lab_features : " + lab_features.count())
    lab_features
  }


  /**
   * Given a feature tuples RDD, construct features in vector
   * format for each patient. feature name should be mapped
   * to some index and convert to sparse feature format.
   * @param sc SparkContext to run
   * @param feature RDD of input feature tuples
   * @return
   */
  def construct(sc: SparkContext, feature: RDD[FeatureTuple]): RDD[(String, Vector)] = {

    /** save for later usage */
    feature.cache()

    /** create a feature name to id map */
    val feature_map = feature.map(x => x._1._2).distinct().sortBy(x => x).collect.zipWithIndex.toMap
    val feature_length = feature_map.size

    /** Transform input feature  such that we map each patient to feature id and feature value tuple
      * Then we group by patient and combine the feature id and value tuples into an array which
      * is then convered to sparse form
      */
    val featuresRDD = feature
                        .map(v => (v._1._1, (feature_map(v._1._2), v._2)))
                        .groupByKey()
                        .map(v => (v._1, Vectors.sparse(feature_length, v._2.toArray))).collect()

    feature.unpersist(false)
    /** The feature vectors returned can be sparse or dense. It is advisable to use sparse */
    sc.parallelize(featuresRDD, 100)
  }
}


