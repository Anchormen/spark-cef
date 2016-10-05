package nl.anchormen.spark.cef

import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import java.text.DateFormat
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types._

object SimpleTest extends App {
  val conf = new SparkConf().setAppName("CEF Test").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  /*
  
  val lines = sc.textFile("src/test/resources/corne.log",1).zipWithIndex().map(_.swap)
  val records = lines.map{case(id, value) => {(id, Array(id, "data", "value", value))}}
  records.take(2).foreach(println)
 
  val hbconf = HBaseConfiguration.create()
  val myTable = new HTable(hbconf, "cef")
  var p = new Put(new String("CEF_1").getBytes());
  p.add(Bytes.toBytes("data"), Bytes.toBytes("column"), Bytes.toBytes("value"));
  myTable.put(p);
  myTable.flushCommits();
  
   */
  val data = sqlContext.read.format("nl.anchormen.spark.cef.CefSource")
    //.option("scanLines", "1")
    .option("partitions", "2")
    .option("year.offset", "2016")
    //.option("end.of.record", "#015")
    .load("src/test/resources/corne.log")
  data.select("eventAnnotationEndTime").rdd.take(10).foreach(println)
  data.printSchema()
  /*
  val fields = data.schema.fields.map(struct => (Bytes.toBytes(struct.name), struct.dataType))
  
  data.rdd.foreachPartition( partition => {
    val hbconf = HBaseConfiguration.create()
    hbconf.set("hbase.zookeeper.quorum", "192.168.15.20"); // enables the client to request hbase information from zookeeper
    hbconf.set("hbase.zookeeper.property.clientPort","2181");
    val myTable = new HTable(hbconf, "cef")
    partition.foreach(row => {  
      var i = 0;
      val p = new Put(Bytes.toBytes("CEF_2016-09-30T13:02:98_"+System.nanoTime()));
      val cfBytes = Bytes.toBytes("data")
      fields.foreach(col => if(!row.isNullAt(i)){
        val value = col._2 match {
          case IntegerType => Bytes.toBytes(row.getInt(i))
          case StringType => Bytes.toBytes(row.getString(i))
          case LongType => Bytes.toBytes(row.getLong(i))
          case FloatType => Bytes.toBytes(row.getFloat(i))
          case DoubleType => Bytes.toBytes(row.getDouble(i))
          case TimestampType => Bytes.toBytes(row.getTimestamp(i).getDate)
          case _ => Bytes.toBytes(row.get(i).toString()) 
        }
        p.add(cfBytes, col._1, value)
        i+=1;
      })
      myTable.put(p);
    })
    myTable.flushCommits();
  })
  */
}