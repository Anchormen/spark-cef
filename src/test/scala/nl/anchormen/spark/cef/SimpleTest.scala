package nl.anchormen.spark.cef

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import java.text.DateFormat

object SimpleTest extends App {
  val conf = new SparkConf().setAppName("CEF Test").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
   
  val data = sqlContext.read.format("nl.anchormen.spark.cef.CefSource")
    //.option("scanLines", "100")
    .option("partitions", "2")
    .load("src/test/resources/simple.cef")
  data.show()
  data.printSchema()
}