package org.example

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.example.loaders.Loader
import org.example.staging.{SmartXmlParser, SmartXmlSchema}
import org.example.utils.ParamsLoad
import scala.collection.immutable.Map
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions.{date_format, to_date}
import java.util.{Calendar, Properties}

object Main {
  def main(args: Array[String]): Unit = {
    val appName: String = "XML-parser-SMART-data"
    val conf = new Configuration()
    implicit val fs: FileSystem = FileSystem.get(conf)
    implicit val parameters: ParamsLoad = new ParamsLoad(args)
    implicit val spark: SparkSession = SparkSession.builder()
      .appName(appName)
      .config("spark.driver.memory", "5g")
      .master("local[*]")
      .getOrCreate()

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

    spark.sparkContext.setLogLevel("ERROR")

    import spark.sqlContext.implicits._

    // read partition after processing kafka
    val xmlDfSmartData = Loader.loadXMLFromParquet(parameters.PATH_INPUT, SmartXmlSchema.schemaRequest,
      parameters.LOAD_DATE_FROM, parameters.LOAD_DATE_TO)

    // parsing XML
    val (targetAll, targetAllVar) = SmartXmlParser.parsingXmlSmartData(xmlDfSmartData)
    val targetAllEvTimeDate = targetAll.withColumn("EVTIME", to_date($"EVTIME", "yyyy-MM-dd"))

    println(Calendar.getInstance().getTime())

    // save to destination
    if (parameters.DESTINATION.equals("POSTGRES")) {
      writeAsPostgresTable(targetAllEvTimeDate, parameters.PARSING_SCHEMA, parameters.PARSING_TABLE, parameters.MPP_USER, parameters.MPP_PASSWORD, parameters.MPP_JDBC_URL)
      writeAsPostgresTable(targetAllVar, parameters.PARSING_SCHEMA, parameters.PARSING_TABLE_VAR, parameters.MPP_USER, parameters.MPP_PASSWORD, parameters.MPP_JDBC_URL)
    } else if (parameters.DESTINATION.equals("GREENPLUM")) {
      writeAsGreenplumTable(targetAllEvTimeDate, parameters.PARSING_SCHEMA, parameters.PARSING_TABLE, parameters.MPP_USER, parameters.MPP_PASSWORD, parameters.MPP_JDBC_URL)
      writeAsGreenplumTable(targetAllVar, parameters.PARSING_SCHEMA, parameters.PARSING_TABLE_VAR, parameters.MPP_USER, parameters.MPP_PASSWORD, parameters.MPP_JDBC_URL)
    } else if (parameters.DESTINATION.equals("HIVE")) {
      writeAsHiveTable(targetAll, parameters.PARSING_SCHEMA, parameters.PARSING_TABLE)
      writeAsHiveTable(targetAllVar, parameters.PARSING_SCHEMA, parameters.PARSING_TABLE_VAR)
    } else if (parameters.DESTINATION.equals("LOCAL")) {
      writeAsLocalFiles(targetAll, parameters.PATH_LOCAL_SAVE, parameters.PARSING_TABLE)
      writeAsLocalFiles(targetAllVar, parameters.PATH_LOCAL_SAVE, parameters.PARSING_TABLE_VAR)
    }

    println(Calendar.getInstance().getTime())

    spark.stop()
  }

  private def writeAsHiveTable(df: DataFrame, schema: String, tablename: String) (implicit spark: SparkSession, parameters: ParamsLoad) = {
    val viewname = "insertOverwriteSparkView"
    df.repartition(1).createOrReplaceTempView(viewname)
    if (tablename.equals(parameters.PARSING_TABLE)) {
      println("... Hive insert overwrite table " + schema + "." + tablename)
      spark.sql(
        s"""
          |INSERT OVERWRITE TABLE $schema.$tablename PARTITION(date_part)
          |SELECT EVNUM, EVTIME, MODEL, CAPACITY_BYTES, SERIAL_NUMBER, FAILURE, DATE_PART
          |FROM $viewname
        """.stripMargin)
    } else if (tablename.equals(parameters.PARSING_TABLE_VAR)) {
      println("... Hive insert overwrite table " + schema + "." + tablename)
      spark.sql(
        s"""
           |INSERT OVERWRITE TABLE $schema.$tablename PARTITION(date_part)
           |SELECT EVNUM, ATTRIBUTES_IND, ATTRIBUTES_NAME, ATTRIBUTES_RAW, DATE_PART
           |FROM $viewname
        """.stripMargin)
    }
  }

  private def  writeAsGreenplumTable(df: DataFrame, schema: String, tablename: String,
                                     user: String, password: String, jdbcUrl: String) (implicit spark: SparkSession, parameters: ParamsLoad) = {
    println("...Greenplum insert table " + schema + "." +tablename)
    val gscWriteOptionMap = Map(
      "url" -> jdbcUrl, // "jdbc:postgresql://localhost:5432/test"
      "user" ->  user, // "testuser"
      "password" -> password, // "testuser"
      "dbschema" -> schema,
      "dbtable" -> tablename
    )
    df.write
      .format("greenplum")
      .options(gscWriteOptionMap)
      .mode(SaveMode.Append)
      .save()
  }

  private def  writeAsPostgresTable(df: DataFrame, schema: String, tablename: String,
                                    user: String, password: String, jdbcUrl: String) (implicit spark: SparkSession, parameters: ParamsLoad) = {
    println("...Postgres insert table " + schema + "." +tablename)
    val connectionProperties = new Properties()
    val driverClassName = "org.postgresql.Driver"
    connectionProperties.put("user", user) // "consumer"
    connectionProperties.put("password", password) // "goto@Postgres1"
    connectionProperties.setProperty("Driver", driverClassName)
    df.write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, schema + "." + tablename, connectionProperties)  // s"jdbc:postgresql://localhost:5432/consumer"
    println(Calendar.getInstance().getTime())
  }

  private def  writeAsLocalFiles(df: DataFrame, path: String, tablename: String) (implicit spark: SparkSession, parameters: ParamsLoad) = {
    println("...write local files " + tablename)
    df.repartition(1)
      .write
      .mode(SaveMode.Append)
      .partitionBy("date_part")
      .parquet(path + "/" + tablename)
  }
}