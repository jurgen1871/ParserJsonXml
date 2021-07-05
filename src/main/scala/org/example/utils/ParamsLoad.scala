package org.example.utils

import scala.collection.immutable.Map

class ParamsLoad(args: Array[String]) extends Serializable {

  private def parseArg(x: String): (String, String) = {
    val arr = x.split("=", -1)
    val key = arr(0)
    val value = arr(1)
    (key, value)
  }

  private val paramMap: Map[String, String] = args.map(parseArg).toMap

  val LOAD_DATE_FROM: Int = paramMap.getOrElse("LOAD_DATE_FROM", "0").toInt
  val LOAD_DATE_TO: Int = paramMap.getOrElse("LOAD_DATE_TO", "99991231").toInt
  val PARSING_SCHEMA: String = paramMap.getOrElse("PARSING_SCHEMA", "no_data")
  val PARSING_TABLE: String = paramMap.getOrElse("PARSING_TABLE", "no_data")
  val PARSING_TABLE_VAR: String = paramMap.getOrElse("PARSING_TABLE_VAR", "no_data")
  val DESTINATION: String = paramMap.getOrElse("DESTINATION", "no_data")
  val PATH_LOCAL_SAVE: String = paramMap.getOrElse("PATH_LOCAL_SAVE", "no_data")
  val PATH_INPUT: String = paramMap.getOrElse("PATH_INPUT", "no_data")
  val MPP_USER: String = paramMap.getOrElse("MPP_USER", "no_data")
  val MPP_PASSWORD: String = paramMap.getOrElse("MPP_PASSWORD", "no_data")
  val MPP_JDBC_URL: String = paramMap.getOrElse("MPP_JDBC_URL", "no_data")

  println("======================================================================")
  println("PROJECT PARAMETERS")
  println("======================================================================")
  println("LOAD_DATE_FROM          = " + LOAD_DATE_FROM)
  println("LOAD_DATE_TO            = " + LOAD_DATE_TO)
  println("PARSING_SCHEMA          = " + PARSING_SCHEMA)
  println("PARSING_TABLE           = " + PARSING_TABLE)
  println("PARSING_TABLE_VAR       = " + PARSING_TABLE_VAR)
  println("DESTINATION             = " + DESTINATION)
  println("PATH_LOCAL_SAVE         = " + PATH_LOCAL_SAVE)
  println("PATH_INPUT              = " + PATH_INPUT)
  println("MPP_USER                = " + MPP_USER)
  println("MPP_PASSWORD            = " + MPP_PASSWORD)
  println("MPP_JDBC_URL            = " + MPP_JDBC_URL)
  println("======================================================================")
}

