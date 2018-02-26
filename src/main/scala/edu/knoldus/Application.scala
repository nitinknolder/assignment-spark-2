package edu.knoldus

import edu.knoldus.Database.{CustomerField, SalesFields}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Application {

  def main (args: Array[String]): Unit = {
    val log: Logger = Logger.getLogger (this.getClass)
    val sparkContext = new SparkContext ("local[4]", "spark-assignment-2")
    val customerRdd: RDD[Array[String]] = sparkContext.textFile ("/home/knoldus/Desktop/SparkAssignment/firstFile.txt").map { x => x.split ('#') }
    val salesRdd: RDD[Array[String]] = sparkContext.textFile ("/home/knoldus/Desktop/SparkAssignment/secondFile.txt").map { y => y.split ('#') }

    val customerRecord = customerRdd.map (x => (x (0).toInt, CustomerField (x (0).toInt, x (1), x (2), x (3))))
    val salesRecord = salesRdd.map (x => (x (1).toInt, SalesFields (TimeConversion.year (x (0).toInt),
      TimeConversion.month (x (0).toInt), TimeConversion.day (x (0).toInt), x (1).toInt, x (2).toLong)))


    val rddJoin = customerRecord.join (salesRecord)
    rddJoin.repartition (1).saveAsTextFile ("/home/knoldus/Desktop/SparkAssignment/Output")

    //    join_rdd.foreach (println)
    sparkContext.stop ()
  }
}



