package nl.anchormen.spark.cef

import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import scala.collection.mutable.Map
import java.text.SimpleDateFormat
import java.sql.Timestamp

object CefRelation{
  // regular expressions used to parse a CEF record and extension field
  //val lineParser = "(.+[^\\\\])\\|(.+[^\\\\])\\|(.+[^\\\\])\\|(.*[^\\\\])\\|(.+[^\\\\])\\|(.+[^\\\\])\\|(.+[^\\\\])\\|(.*)".r
  val lineParser = "(.*(?<!\\\\))\\|(.*(?<!\\\\))\\|(.*(?<!\\\\))\\|(.*(?<!\\\\))\\|(.*(?<!\\\\))\\|(.*(?<!\\\\))\\|(.*(?<!\\\\))\\|(.*)".r
  val extensionPattern = "(\\w+)=(.+?)(?=$|\\s\\w+=)".r 
  val syslogDate = new SimpleDateFormat("MMM dd HH:mm:ss")
  
  // set with datetime patterns used within CEF
  val sdfPatterns = Array(
      new SimpleDateFormat("MMM dd HH:mm:ss.SSS zzz"),
      new SimpleDateFormat("MMM dd HH:mm:ss.SSS"),
      new SimpleDateFormat("MMM dd HH:mm:ss zzz"),
      syslogDate,
      new SimpleDateFormat("MMM dd yyyy HH:mm:ss.SSS zzz"),
      new SimpleDateFormat("MMM dd yyyy HH:mm:ss.SSS"),
      new SimpleDateFormat("MMM dd yyyy HH:mm:ss zzz"),
      new SimpleDateFormat("MMM dd yyyy HH:mm:ss")
      )
}

case class CefRelation(lines: RDD[String], 
    var cefSchema : StructType = null, 
    scanLines : Integer = -1,
    params: scala.collection.immutable.Map[String, String])
  (@transient val sqlContext: SQLContext) extends BaseRelation with TableScan {
  
  
  val endOfRecord = params.getOrElse("end.of.record", "")
  val yearOffset = new SimpleDateFormat("yyyy").parse(params.getOrElse("year.offset", "1970")).getTime
  
  var extIndex : Map[String, Int] = null // holds an index from key name to row index
  var sdfPatterns : Map[String, Option[SimpleDateFormat]] = null // holds the pattern to use to parse timestamps (None == long)
  
  override def schema : StructType = inferSchema
  
  /**
   * Infers the schema of the data using the scanLines top number of lines
   */
  private def inferSchema(): StructType = {
    if (this.cefSchema == null) {
      val tuple = InferSchema(lines, scanLines)
      cefSchema = tuple._1
      extIndex = tuple._2
      sdfPatterns = tuple._3
    }
    cefSchema
  }
  
  /**
   * Converts the CEF records to Row elements using the inferred schema.
   */
  override def buildScan(): RDD[Row] = {
    lines.map(parseLine(_))
  }

  /**
   * Converts a single line into a Row object as defined by the inferred schema
   */
  def parseLine(lineToParse : String) : Row = {
     val line = if(endOfRecord.length > 0 && lineToParse.endsWith(endOfRecord))
         lineToParse.substring(0, lineToParse.length() - endOfRecord.length()) else  lineToParse
     val mOpt = CefRelation.lineParser.findFirstMatchIn(line) // using regex might be slow, TODO: faster?
      val values = Array.fill[Any](schema.length)(None) // initial set of values containing 'None'
      if(mOpt.isDefined){
        val matc = mOpt.get
        for(i <- 1 to matc.groupCount){ // iterate all elements found in the line
           if(i==1){ // parse optional syslog elements and set the CEF version
             val group1 = matc.group(i).trim
             if(!group1.startsWith("CEF")){
               val syslogAndCEF = group1.split("CEF")
               values(i-1) = syslogAndCEF(1).replace(":","").trim.toInt
               val date = syslogAndCEF(0).substring(0, syslogAndCEF(0).trim.lastIndexOf(' ')).trim
               values(extIndex(InferSchema.syslogDate)) = new Timestamp( CefRelation.syslogDate.parse(date).getTime + yearOffset)
               val host = syslogAndCEF(0).substring(syslogAndCEF(0).trim.lastIndexOf(' '))
               values(extIndex(InferSchema.syslogHost)) = host
             }else{
              values(i-1) = group1.substring(4).trim.toInt
             }
          } else if(i == 8 && matc.group(i).trim.length() > 0){ // parse Extension field and cast it to the right type
            CefRelation.extensionPattern.findAllIn(matc.group(i)).matchData.foreach(m => {
              val key = m.group(1)
              val value = m.group(2)
              val index = extIndex(key)
              values(index) = schema.apply(key).dataType match { // handle the value based on its inferred type
                case FloatType => value.toFloat
                case DoubleType => value.toDouble
                case IntegerType => value.toInt
                case LongType => value.toLong
                case TimestampType => { // try to parse timestamps
                  val pattern = sdfPatterns.getOrElse(key, None)
                  if(pattern.isDefined) new Timestamp(pattern.get.parse(value).getTime)
                  else new Timestamp(value.toLong)
                }
                case _ => value.replaceAll("\\\\=", "=") // if nothing else we assume the value is a string
                  .replaceAll("\\\\n", sys.props("line.separator") )
                  .replaceAll("\\\\\\\\", "\\\\")
              }
            }) 
          } else values(i-1) = matc.group(i).replaceAll("\\\\\\|", "|")
            .replaceAll("\\\\n", sys.props("line.separator") )
            .replaceAll("\\\\\\\\", "\\\\")
        }
      }
      Row.fromSeq(values) // all 'None' if mOpt was not defined (i.e. on parse error) TODO handle this situation
  }
}