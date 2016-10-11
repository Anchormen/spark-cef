package nl.anchormen.spark.cef

import org.apache.spark.sql.types.StructType
import org.apache.spark.rdd.RDD
import scala.util.matching.Regex
import org.apache.spark.sql.types._
import scala.collection.mutable.{ArrayBuffer, HashMap}
import java.text.SimpleDateFormat


object InferSchema {
  
  //val syslogDate = "Syslog_Date"
  //val syslogHost = "Syslog_Host"
  
  // CEF Standard header fields. Extension fields will be added to this base set by apply(...)
  val fields = ArrayBuffer(
    StructField("CEF_Version", IntegerType, nullable = true),
    StructField("CEF_DeviceVendor", StringType, nullable = true),
    StructField("CEF_DeviceProduct", StringType, nullable = true),
    StructField("CEF_DeviceVersion", StringType, nullable = true),
    StructField("CEF_DeviceEventClassID", StringType, nullable = true),
    StructField("CEF_Name", StringType, nullable = true),
    StructField("CEF_Severity", StringType, nullable = true))
  
  // All types of non-string extensions within CEF   
  val extensions = Map(
    "cfp1" -> StructField("cfp1", FloatType, nullable = true),
    "cfp2" -> StructField("cfp2", FloatType, nullable = true),
    "cfp3" -> StructField("cfp3", FloatType, nullable = true),
    "cfp4" -> StructField("cfp4", FloatType, nullable = true),
    "cnt1" -> StructField("cnt1", LongType, nullable = true),
    "cnt2" -> StructField("cnt2", LongType, nullable = true),
    "cnt3" -> StructField("cnt3", LongType, nullable = true),
    "cnt" -> StructField("cnt", IntegerType, nullable = true),
    "destinationTranslatedPort" -> StructField("destinationTranslatedPort", IntegerType, nullable = true),
    "deviceDirection" -> StructField("deviceDirection", IntegerType, nullable = true),
    "dpid" -> StructField("dpid", IntegerType, nullable = true),
    "dpt" -> StructField("dpt", IntegerType, nullable = true),
    "dvcpid" -> StructField("dvcpid", IntegerType, nullable = true),
    "flexNumber1" -> StructField("flexNumber1", LongType, nullable = true),
    "flexNumber2" -> StructField("flexNumber2", LongType, nullable = true),
    "fsize" -> StructField("fsize", IntegerType, nullable = true),
    "in" -> StructField("in", IntegerType, nullable = true),
    "oldFileSize" -> StructField("oldFileSize", IntegerType, nullable = true),
    "out" -> StructField("out", IntegerType, nullable = true),
    "sourceTranslatedPort" -> StructField("sourceTranslatedPort", IntegerType, nullable = true),
    "spid" -> StructField("spid", IntegerType, nullable = true),
    "spt" -> StructField("spt", IntegerType, nullable = true),
    "type" -> StructField("type", IntegerType, nullable = true),
    "dlat" -> StructField("dlat", DoubleType, nullable = true),
    "dlong" -> StructField("dlong", DoubleType, nullable = true),
    "eventId" -> StructField("eventId", LongType, nullable = true),
    "slat" -> StructField("slat", DoubleType, nullable = true),
    "slong" -> StructField("slong", DoubleType, nullable = true),
    
    // Timestamps 
    "deviceCustomDate1"-> StructField("deviceCustomDate1", TimestampType, nullable = true),
    "deviceCustomDate2"-> StructField("deviceCustomDate2 d", TimestampType, nullable = true),
    "end"-> StructField("end", TimestampType, nullable = true),
    "fileCreateTime"-> StructField("fileCreateTime", TimestampType, nullable = true),
    "fileModificationTime"-> StructField("fileModificationTime", TimestampType, nullable = true),
    "flexDate1"-> StructField("flexDate1", TimestampType, nullable = true),
    "oldFileCreateTime"-> StructField("oldFileCreateTime", TimestampType, nullable = true),
    "oldFileModificationTime"-> StructField("oldFileModificationTime", TimestampType, nullable = true),
    "rt"-> StructField("rt", TimestampType, nullable = true),
    "start"-> StructField("start", TimestampType, nullable = true),
    "art"-> StructField("art", TimestampType, nullable = true)
    )

  /** 
   *  Infers the schema of the provided CEF formatted data. It return three structures:
   *   - the schema
   *   - a map from fieldname to column index
   *   _ a map from fieldname to SimpleDateTime pattern (only for Timestamp fields)  
   */
  def apply(lines: RDD[String], params: scala.collection.immutable.Map[String, String]) 
      : Tuple3[StructType, HashMap[String, Int], HashMap[String, Option[SimpleDateFormat]]] = {
    val scanLines = params.getOrElse("scanlines", "-1").toInt
    val epochMillisFields = if(params.contains("epoch.millis.fields"))
        params.get("epoch.millis.fields").get.split(",").map(_.trim)
      else Array[String]()
    if(scanLines > 0){ 
      // perform schema inference in the driver by taking scanLines number of lines
      infer(lines.take(scanLines).toIterator, epochMillisFields)
    }else{
      // perform inference per partition and merge the result
     val iter = lines.mapPartitions(part => Array(infer(part, epochMillisFields)).toIterator, false)
     
     // let the driver combine the partial results
     val partialResults = iter.collect()
     var schema = partialResults(0)._1
     val extIndex = partialResults(0)._2
     val datetimePatterns = partialResults(0)._3
     var fields = schema.fieldNames
     for(i <- 1 until partialResults.size){
       partialResults(i)._1.fieldNames.foreach(field => {
    		   if(!fields.contains(field)){
    			   extIndex += field -> schema.length
    		     schema = schema.add(partialResults(i)._1.apply(field))
    		     fields = schema.fieldNames
    		     if(partialResults(i)._3.contains(field)){
    		       datetimePatterns += field -> partialResults(i)._3.get(field).get
    		     }
    		   }
       })
     }
     (schema, extIndex, datetimePatterns)
    }
  }
  
  /**
   * Infers the schema on part of the data
   */
  def infer(collection : Iterator[String], epochFields : Array[String]) = {
    val myFields = fields.clone() // holds all fields found 
    val extIndex = new HashMap[String, Int]() // holds the global mapping from key to row index
    val datetimePatterns = new HashMap[String, Option[SimpleDateFormat]]() // TODO: maybe add a list of dateformats to support multiple variations
    
    epochFields.foreach( name => {
      extIndex += name -> myFields.length
      myFields += StructField(name, TimestampType, nullable = true)
    } )
   collection.foreach(line => {
      val mOpt = CefRelation.lineParser.findFirstMatchIn(line)
      if(mOpt.isDefined && mOpt.get.groupCount >= 8){ // interpret the extension if it exists
        /*
        if(!mOpt.get.group(1).trim.startsWith("CEF") && !extIndex.contains(syslogDate)){ // assume syslog information if the line does not start with 'CEF'
          extIndex += syslogDate -> myFields.length
          myFields += StructField(syslogDate, TimestampType, nullable = true)
          extIndex += syslogHost -> myFields.length
          myFields += StructField(syslogHost, StringType, nullable = true)
        }
        */
        CefRelation.extensionPattern.findAllIn(mOpt.get.group(8)).matchData.foreach(m => {
            val key = m.group(1)
            if(!extIndex.contains(key)){
              extIndex += key -> myFields.length
              myFields += extensions.getOrElse(key, StructField(key, StringType, nullable = true)) // set the type of the key (StringType by default)
            }
            // in case of a Timestamp we have to figure out how it is formatted
            if(extensions.contains(key) && extensions.get(key).get.dataType == TimestampType && !datetimePatterns.contains(key)){
            	val value = m.group(2)
              var pattern : SimpleDateFormat = null
              CefRelation.sdfPatterns.foreach(p => if(pattern == null) try {
                 p.parse(value)
                 pattern = p;
              }catch{
                case _ : Throwable => None // just try the next pattern on Exception
              })
              
              if(pattern == null)
                datetimePatterns += key -> None // None implies it is a Long (epoch) and not a string
              else
                datetimePatterns += key -> Some(pattern)
           }
          })
      }
    })
    (StructType(myFields), extIndex, datetimePatterns)
  }
  
}