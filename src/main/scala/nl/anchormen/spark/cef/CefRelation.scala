package nl.anchormen.spark.cef

import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import scala.collection.mutable.Map
import java.text.SimpleDateFormat
import java.sql.Timestamp
import scala.util.Try
import org.slf4j.LoggerFactory
import scala.util.Success
import scala.util.Failure
import java.io.StringWriter
import java.io.PrintWriter

object CefRelation{
  // regular expressions used to parse a CEF record and extension field
  val lineParser = "(.*(?<!\\\\))\\|(.*(?<!\\\\))\\|(.*(?<!\\\\))\\|(.*(?<!\\\\))\\|(.*(?<!\\\\))\\|(.*(?<!\\\\))\\|(.*(?<!\\\\))\\|(.*)".r
  val extensionPattern = "([\\w|\\.|_|-]+)=(.+?)(?=$|\\s[\\w|\\.|_|-]+=)".r 

  //val syslogDate = new SimpleDateFormat("yyyy MMM dd HH:mm:ss")
  
  // set with datetime patterns used within CEF
  val sdfPatterns = Array(
      new SimpleDateFormat("MMM dd HH:mm:ss.SSS zzz"),
      new SimpleDateFormat("MMM dd HH:mm:ss.SSS"),
      new SimpleDateFormat("MMM dd HH:mm:ss zzz"),
      new SimpleDateFormat("yyyy MMM dd HH:mm:ss"),
      new SimpleDateFormat("MMM dd yyyy HH:mm:ss.SSS zzz"),
      new SimpleDateFormat("MMM dd yyyy HH:mm:ss.SSS"),
      new SimpleDateFormat("MMM dd yyyy HH:mm:ss zzz"),
      new SimpleDateFormat("MMM dd yyyy HH:mm:ss")
      )
   
   /**
    * Un-escapes CEF string values according to the specification   
    */
   def cleanString(value : String) = value.replaceAll("\\\\\\|", "|").replaceAll("\\\\n", sys.props("line.separator") ).replaceAll("\\\\\\\\", "\\\\")
      
   /**
    * Parses a single line without the use of a schema (used for streaming applications)
    */
   def parseLine(lineToParse : String, endOfRecord:String = "", longs : Set[String] = Set[String](), doubles : Set[String] = Set[String]()) = Try{
     var line = lineToParse.substring(lineToParse.indexOf("CEF"))
     line = if(endOfRecord.length > 0 && line.endsWith(endOfRecord))
     line.substring(0, line.length() - endOfRecord.length()) else line
     val row = Map[String, Any]()
     val mOpt = CefRelation.lineParser.findFirstMatchIn(line) // using regex might be slow, TODO: faster?
     if(mOpt.isDefined){
        val matc = mOpt.get
        for(i <- 1 to matc.groupCount){ // iterate all elements found in the line
          i match {
            case 1 => row += "CEF_Version" -> matc.group(i).trim.substring(4).trim.toInt
            case 2 => row += "CEF_DeviceVendor" -> cleanString(matc.group(i))
            case 3 => row += "CEF_DeviceProduct" -> cleanString(matc.group(i))
            case 4 => row += "CEF_DeviceVersion" -> cleanString(matc.group(i))
            case 5 => row += "CEF_DeviceEventClassID" -> cleanString(matc.group(i))
            case 6 => row += "CEF_Name" -> cleanString(matc.group(i))
            case 7 => row += "CEF_Severity" -> cleanString(matc.group(i))
            case 8 => if(matc.group(i).trim.length() > 0){
                CefRelation.extensionPattern.findAllIn(matc.group(i)).matchData.foreach(m => {
                val key = m.group(1)
                val value = m.group(2)
                if(longs.contains(key)){
                  row += key -> value.toLong
                }else if(doubles.contains(key)){
                	row += key -> value.toDouble
                }else{
                	row += key -> value
                }
              })
            }
          }
        }
      }
      row
  }
      
}

case class CefRelation(lines: RDD[String], 
    var cefSchema : StructType = null, 
    params: scala.collection.immutable.Map[String, String])
  (@transient val sqlContext: SQLContext) extends BaseRelation with TableScan {
  
  private val logger = LoggerFactory.getLogger(CefRelation.getClass)
  
  val endOfRecord = params.getOrElse("end.of.record", "")
  val yearOffset = params.getOrElse("year.offset", "1970")
  val trimStrings = params.getOrElse("string.trim", "false").toBoolean
  
  var extIndex : Map[String, Int] = null // holds an index from key name to row index
  var sdfPatterns : Map[String, Option[SimpleDateFormat]] = null // holds the pattern to use to parse timestamps (None == long)
  
  override def schema : StructType = inferSchema
  
  /**
   * Infers the schema of the data using the scanLines top number of lines
   */
  private def inferSchema(): StructType = {
    if (this.cefSchema == null) {
      val tuple = InferSchema(lines, params)
      cefSchema = tuple._1
      extIndex = tuple._2
      sdfPatterns = tuple._3
      if(params.getOrElse("exception.add.result", "false").toBoolean){
        cefSchema = cefSchema.add( new StructField("parse_exception", StringType, nullable = true) )
      }
    }
    cefSchema
  }
  
  /**
   * Converts the CEF records to Row elements using the inferred schema.
   */
  override def buildScan(): RDD[Row] = {
    if(params.getOrElse("ignore.exception", "false").toBoolean){
      // parse lines and handle exceptions
    	val parseResult = lines.map(tryParseLine(_))
    	  
    	parseResult.map( t => t match {
    	  case Success(r) => r
    	  case Failure(t) => {
    	    if(params.getOrElse("exception.log", "true").toBoolean) logger.warn("Unable to parse line", t)
    	    val values = Array.fill[Any](schema.length)(None)
    	    if(params.getOrElse("exception.add.result", "false").toBoolean){
      	    val sw = new StringWriter();
            t.printStackTrace(new PrintWriter(sw));
      	    values(values.length - 1) = sw.toString()
    	    }
    	    Row.fromSeq(values)
    	  }
    	})
    }else{
      // parse lines and fail fast on an exception
    	lines.map(parseLine(_))
    }
  }

  /**
   * Ads a Try arround to parseLine in order to make parsing more robust
   */
  def tryParseLine(lineToParse : String) : Try[Row] = Try(parseLine(lineToParse))
  
  /**
   * Converts a single line into a Row object as defined by the inferred schema
   */
  def parseLine(lineToParse : String) : Row = {
     var line = lineToParse.substring(lineToParse.indexOf("CEF"))
     line = if(endOfRecord.length > 0 && line.endsWith(endOfRecord))
     line.substring(0, line.length() - endOfRecord.length()) else line
     
     val mOpt = CefRelation.lineParser.findFirstMatchIn(line) // using regex might be slow, TODO: faster?
      val values = Array.fill[Any](schema.length)(None) // initial set of values containing 'None'
      if(mOpt.isDefined){
        val matc = mOpt.get
        for(i <- 1 to matc.groupCount){ // iterate all elements found in the line
           if(i==1){ // parse optional syslog elements and set the CEF version
             val group1 = matc.group(i).trim
             /*
             if(!group1.startsWith("CEF")){
               val syslogAndCEF = group1.split("CEF")
               values(i-1) = syslogAndCEF(1).replace(":","").trim.toInt
               val date = yearOffset+" "+syslogAndCEF(0).substring(0, syslogAndCEF(0).trim.lastIndexOf(' ')).trim
               values(extIndex(InferSchema.syslogDate)) = new Timestamp( CefRelation.syslogDate.parse(date).getTime)
               val host = syslogAndCEF(0).substring(syslogAndCEF(0).trim.lastIndexOf(' '))
               values(extIndex(InferSchema.syslogHost)) = host
             }else{
             */
              values(i-1) = group1.substring(4).trim.toInt
             //}
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
                case _ => {
                  val cleaned = value.replaceAll("\\\\=", "=") // if nothing else we assume the value is a string
                  .replaceAll("\\\\n", sys.props("line.separator") )
                  .replaceAll("\\\\\\\\", "\\\\")
                  if(trimStrings) cleaned.trim() else cleaned
                }
              }
            }) 
          } else values(i-1) = CefRelation.cleanString(matc.group(i))
        }
      }
      Row.fromSeq(values) // all 'None' if mOpt was not defined (i.e. on parse error) TODO handle this situation
  }
}