package org.example.loaders

import com.databricks.spark.xml.XmlReader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import scala.xml.XML
import scala.collection.immutable.Map

object Loader {

  case class SMART_ROW(EVNUM: String,
                       EVTIME: String,
                       XML_DATA: String,
                       DATE_PART: Long)

  def loadXMLFromParquet (path: String, schema: StructType, loadDateFrom: Int, loadDateTo: Int)(implicit spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._

    val parqDF = spark
      .read
      .parquet(path)
      .filter($"DATE_PART" >= loadDateFrom && $"DATE_PART" < loadDateTo)
    parqDF.show(2,false)

    // filter XML_DATA not null
    val parqFiltDF = parqDF.filter($"XML_DATA".isNotNull)

    // wrapped json to xml
    val dsXML = parqFiltDF.as[SMART_ROW].map(wrapIntoXml)
    println("Wrapped XML DF")
    dsXML.show(2,false)
    dsXML.printSchema()

    // read XML
    val xr = new XmlReader()
      .withRowTag("ROW_TAG")
      .withSchema(schema)
      .withTreatEmptyValuesAsNulls(true)
      .withIgnoreSurroundingSpaces(true)
    val dfXml = xr.xmlDataset(spark, dsXML)
    dfXml.show(2,false)
    dfXml.printSchema()
    //dfXml.cache()
    dfXml
  }

  private def wrapIntoXml(smartrow: SMART_ROW) = {
    val wrappedRow = <ROW_TAG>
      <EVNUM>{smartrow.EVNUM}</EVNUM>
      <EVTIME>{smartrow.EVTIME}</EVTIME>
      {XML.loadString(smartrow.XML_DATA)}
      <DATE_PART>{smartrow.DATE_PART}</DATE_PART>
    </ROW_TAG>
    wrappedRow.toString()
  }
}
