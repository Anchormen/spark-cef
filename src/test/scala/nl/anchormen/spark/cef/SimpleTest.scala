package nl.anchormen.spark.cef

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import java.text.DateFormat
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types._

object SimpleTest extends App {
  val conf = new SparkConf().setAppName("CEF Test").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val data = sqlContext.read.format("nl.anchormen.spark.cef.CefSource")
    .option("partitions", "2")
    .option("schema.lines", "-1")
    .option("end.of.record", "#015")
    .option("string.trim", "true")
    .option("ignore.exception", "true")
    .option("exception.log", "false")
    .option("exception.add.result", "true")
    .option("epoch.millis.fields", "mrt,rt")
    .load("src/test/resources/simple.cef")
  data.printSchema()
  data.show()
  data.filter("parse_exception IS NOT NULL").select("parse_exception").rdd.take(10).foreach(println)
  
  // data.coalesce(1).write.parquet("src/test/output.parquet")
  //val data = sc.textFile("src/test/resources/corne.log").filter(_.contains("CEF")).map(CefRelation.parseLine(_, "#015", Set("mrt", "art"), Set("slong", "slat", "dlong", "dlat")))
  //data.filter(_.isSuccess).map(m => scala.util.parsing.json.JSONObject(m.get.toMap)).collect.foreach(println)
  /*
  // convert DF to json and write it to MaprDB (Json documents)  
  data.toJSON.foreachPartition( part => {
    val table = MapRDB.getTable("tablename")
     part.foreach(table.insertOrReplace(MapRDB.newDocument(_))
  })
  */
  /*
  // write data to MapR-DB (binary)
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