/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.clustering

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object Metrics {
  /**
   * Given input RDD with tuples of assigned cluster id by clustering,
   * and corresponding real class. Calculate the purity of clustering.
   * Purity is defined as
   *             \fract{1}{N}\sum_K max_j |w_k \cap c_j|
   * where N is the number of samples, K is number of clusters and j
   * is index of class. w_k denotes the set of samples in k-th cluster
   * and c_j denotes set of samples of class j.
   * @param clusterAssignmentAndLabel RDD in the tuple format
   *                                  (assigned_cluster_id, class)
   * @return purity
   */
  def purity(clusterAssignmentAndLabel: RDD[(Int, Int)]): Double = {
    clusterAssignmentAndLabel.cache()
    val N = clusterAssignmentAndLabel.count()
    // 1. Group by class assignments per cluster
    // 2. Count assignments per class per cluster,
    val clusterCounts = clusterAssignmentAndLabel.map(f => ((f._1, f._2), 1.0))
                                                 .reduceByKey(_+_)

//    println("Cluster counts:")
//    clusterCounts.collect().map(f => println("cluster: " + f._1._1 + " label: " + f._1._2 + " count: " + f._2))

    // 1. map clusters to cluster sum per class
    // 2. extract max per cluster class count,
    // 3. Add max class count per cluster
    // 4. Divide by num records to get purity
    val purity = clusterCounts.map(f => (f._1._1, f._2))
                              .reduceByKey((v1, v2) => if (v1 > v2) v1 else v2)
                              .map(f => (0, f._2))
                              .reduceByKey(_+_).values.first()/N
    clusterAssignmentAndLabel.unpersist(false)
    purity
  }
}
