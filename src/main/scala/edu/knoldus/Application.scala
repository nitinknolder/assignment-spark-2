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

    val yearlyRecord = rddJoin.map(x => ((x._2._1.state, x._2._2.year), x._2._2.salesPrice))
      .reduceByKey(_ + _).map(y => s"${y._1._1}#${y._1._2}###${y._2}")

    val monthlyRecord = rddJoin.map(x => ((x._2._1.state, x._2._2.year, x._2._2.month), x._2._2.salesPrice))
      .reduceByKey(_ + _).map(y => s"${y._1._1}#${y._1._2}#${y._1._3}##${y._2}")

    val dailyRecord = rddJoin.map(x => ((x._2._1.state, x._2._2.year, x._2._2.month, x._2._2.day), x._2._2.salesPrice))
      .reduceByKey(_ + _).map(y => s"${y._1._1}#${y._1._2}#${y._1._3}#${y._1._4}#${y._2}")

    val result= yearlyRecord union  monthlyRecord union  dailyRecord

    result.repartition(1).saveAsTextFile("/home/knoldus/Desktop/SparkAssignment/outputFile.txt")


    sparkContext.stop ()
  }
}



