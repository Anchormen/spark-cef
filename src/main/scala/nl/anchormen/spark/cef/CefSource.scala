package nl.anchormen.spark.cef

import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType

class CefSource extends RelationProvider with SchemaRelationProvider {
  
  def createRelation(sqlContext: SQLContext, params: Map[String, String], schema: StructType): BaseRelation = {
    if(!params.contains("path")) throw new Exception("'path' must be specified for CEF data.")
    if(schema != null) throw new IllegalArgumentException("Setting a schema is not supported")
    val path = params("path")
    val scanLines = params.getOrElse("scanlines", "-1").toInt
    val lines = if(params.contains("partitions") )
          sqlContext.sparkContext.textFile(path, params("partitions").toInt)
        else 
          sqlContext.sparkContext.textFile(path)
          
    CefRelation(lines.filter(_.contains("CEF")), schema, scanLines, params)(sqlContext)
  }

  def createRelation(sqlContext: SQLContext, params: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, params, null)
  }
  
}