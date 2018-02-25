package edu.knoldus

import edu.knoldus.Database.{CustomerField, SalesFields}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Application {

  def main (args: Array[String]): Unit = {
    val log: Logger = Logger.getLogger (this.getClass)
    val sparkConf: SparkConf = new SparkConf ().setMaster ("local[*]").setAppName ("spark-Assignment2")
    val sparkContext = new SparkContext (sparkConf)
    val customerRdd: RDD[Array[String]] = sparkContext.textFile ("/home/knoldus/Desktop/SparkAssignment/firstFile.txt").map { x => x.split ('#') }
    val salesRdd: RDD[Array[String]] = sparkContext.textFile ("/home/knoldus/Desktop/SparkAssignment/secondFile.txt").map { y => y.split ('#') }

    val fourthIndex = 4
    val fifthIndex = 5
    val customerRecord = customerRdd.map (x => (x (0).toInt, CustomerField (x (0).toInt, x (1), x (2), x (3),
      x (fourthIndex), x (fifthIndex).toInt)))
    val salesRecord = salesRdd.map (x => (x (1).toInt, SalesFields (TimeConversion.year (x (0).toInt),
      TimeConversion.month (x (0).toInt), TimeConversion.day (x (0).toInt), x (1).toInt, x (2).toLong)))
    val join_rdd = customerRecord.join (salesRecord)
    join_rdd.foreach (log.info)
    sparkContext.stop ()
  }
}



