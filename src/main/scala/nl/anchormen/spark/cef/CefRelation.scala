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
  val lineParser = "(.+[^\\\\])\\|(.+[^\\\\])\\|(.+[^\\\\])\\|(.+[^\\\\])\\|(.+[^\\\\])\\|(.+[^\\\\])\\|(.+[^\\\\])\\|(.*)".r
  val extensionPattern = "(\\w+)=(.+?)(?=$|\\s\\w+=)".r 
  // set with datetime patterns used within CEF
  val sdfPatterns = Array(
      new SimpleDateFormat("MMM dd HH:mm:ss.SSS zzz"),
      new SimpleDateFormat("MMM dd HH:mm:ss.SSS"),
      new SimpleDateFormat("MMM dd HH:mm:ss zzz"),
      new SimpleDateFormat("MMM dd HH:mm:ss"),
      new SimpleDateFormat("MMM dd yyyy HH:mm:ss.SSS zzz"),
      new SimpleDateFormat("MMM dd yyyy HH:mm:ss.SSS"),
      new SimpleDateFormat("MMM dd yyyy HH:mm:ss zzz"),
      new SimpleDateFormat("MMM dd yyyy HH:mm:ss")
      )
}

case class CefRelation(lines: RDD[String], 
    var cefSchema : StructType = null, 
    scanLines : Integer = -1)
  (@transient val sqlContext: SQLContext) extends BaseRelation with TableScan {
  
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
    lines.filter(_.trim().length > 7) // only parse lines which have sufficient length
     .map(line => {
      val mOpt = CefRelation.lineParser.findFirstMatchIn(line) // using regex might be slow, TODO: faster?
      val values = Array.fill[Any](schema.length)(None) // initial set of values containing 'None'
      if(mOpt.isDefined){
        val matc = mOpt.get
        for(i <- 1 to matc.groupCount){ // iterate all elements found in the line
           if(i==1){ // set the CEF version
            values(i-1) = matc.group(i).substring(4).toInt
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
    })
  }
}