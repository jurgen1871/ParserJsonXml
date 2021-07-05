package org.example.staging

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

object SmartXmlSchema {
  // XML schema
  private val INFO = StructField("INFO", StructType(Seq(
    StructField("model", StringType) //  SMARTDocument/INFO/model
    , StructField("capacity_bytes", LongType) //  SMARTDocument/INFO/capacity_bytes
    , StructField("serial_number", StringType) //  SMARTDocument/INFO/serial_number
    , StructField("failure", IntegerType) //  SMARTDocument/INFO/failure
  )))

  private val ATTRIBUTES = StructField("ATTRIBUTES", StructType(Seq(
    StructField("ATTRIBUTES_NAME", StringType) //  SMARTDocument/ATTRIBUTES/ATTRIBUTES_NAME
    , StructField("ATTRIBUTES_RAW", StringType) //  SMARTDocument/ATTRIBUTES/ATTRIBUTES_RAW
  )))

  val schemaRequest = StructType( Seq(
    StructField("EVNUM", StringType)
    , StructField("EVTIME", StringType)
    , StructField("SMARTDocument", StructType(Seq(
      INFO
      , ATTRIBUTES
    )))
    , StructField("DATE_PART", LongType)
  ))
}
