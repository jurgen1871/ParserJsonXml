package org.example.staging

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{explode, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.regex.{Matcher, Pattern}
import scala.collection.mutable

/*  ===================================================================================
 *   Parsing linked arrays of variable length from an XML structure using regexp UDF
 *  ===================================================================================*/
object SmartXmlParser {
  def parsingXmlSmartData(df: DataFrame)(implicit spark: SparkSession): (DataFrame, DataFrame) = {
    import spark.sqlContext.implicits._

    // register UDF
    def fillMapUDF: UserDefinedFunction = udf((n: String) => fillMapXML(n))

    // parsing static XML schema
    println("targetDf")
    val targetDf = df.withColumn("INFO", $"SMARTDocument.INFO")
      .withColumn("ATTRIBUTES", $"SMARTDocument.ATTRIBUTES")
      .select(
        $"EVNUM"
        , $"EVTIME"
        , $"INFO.model".as("MODEL")
        , $"INFO.capacity_bytes".as("CAPACITY_BYTES")
        , $"INFO.serial_number".as("SERIAL_NUMBER")
        , $"INFO.failure".as("FAILURE")
        , $"ATTRIBUTES.ATTRIBUTES_NAME".as("ATTRIBUTES_NAME")
        , $"ATTRIBUTES.ATTRIBUTES_RAW".as("ATTRIBUTES_RAW")
        , $"DATE_PART"
      )
    targetDf.show(2,false)
    targetDf.printSchema()

    // dataframe ATTRIBUTES_ is not null
    println("targetVar")
    val targetVar = targetDf
      .filter("ATTRIBUTES_NAME is not NULL and ATTRIBUTES_RAW is not NULL")
    targetVar.show(2,false)
    targetVar.printSchema()

    targetVar.cache()

    // ATTRIBUTES_NAME
    // parse XML to map
    val parsedMapVarWindow = targetVar.select($"EVNUM", $"ATTRIBUTES_NAME")
      .withColumn("map_var", fillMapUDF($"ATTRIBUTES_NAME"))
    parsedMapVarWindow.show(2,false)
    parsedMapVarWindow.printSchema()

    // explode map to rows
    val exlodedVarWindow = parsedMapVarWindow.select($"EVNUM"
      , explode($"map_var"))
      .withColumnRenamed("value","ATTRIBUTES_NAME")
    exlodedVarWindow.show(2,false)
    exlodedVarWindow.printSchema()

    // ATTRIBUTES_RAW
    // parse XML to map
    val parsedMapVarName = targetVar.select($"EVNUM", $"ATTRIBUTES_RAW")
      .withColumn("map_var", fillMapUDF($"ATTRIBUTES_RAW"))
    parsedMapVarName.show(2,false)
    parsedMapVarName.printSchema()

    // explode map to rows
    val exlodedVarName = parsedMapVarName.select($"EVNUM"
      , explode($"map_var"))
      .withColumnRenamed("value","ATTRIBUTES_RAW")
    exlodedVarName.show(2,false)
    exlodedVarName.printSchema()

    println("Join all var")
    val targetAllVar = exlodedVarWindow
      .join(exlodedVarName, Seq("EVNUM", "key"),"outer")
      .withColumnRenamed("key", "ATTRIBUTES_IND")
    targetAllVar.printSchema()
    targetAllVar.show(2,false)

    val targetAll = targetVar.select($"EVNUM", $"EVTIME", $"MODEL", $"CAPACITY_BYTES", $"SERIAL_NUMBER", $"FAILURE")

    (targetAll, targetAllVar)
  }

  /* ---------------------------------------------------------------------------------
     UDF parse XML to map ( 1 -> w1, 2 -> w2, ... , N -> wN)
     ---------------------------------------------------------------------------------*/
  private def fillMapXML(colValue: String): mutable.HashMap[Int, String] = {
    val map = new mutable.HashMap[Int, String]
    if (colValue != null && colValue.nonEmpty) {
      // simple search for all occurrences with a lazy quantifier "*?"
      val p: Pattern = Pattern.compile("<I(\\d{1,})>.*?<value>(.*?)<\\/value>")
      val m: Matcher = p.matcher(colValue)
      while (m.find()) {
        // capturing group 1 - key , 2 - value
        // do not process an empty key
        if (m.start(1) < m.end(1)) {
          map += (m.group(1).toInt -> m.group(2))
        }
      }
    }
    map
  }
}
