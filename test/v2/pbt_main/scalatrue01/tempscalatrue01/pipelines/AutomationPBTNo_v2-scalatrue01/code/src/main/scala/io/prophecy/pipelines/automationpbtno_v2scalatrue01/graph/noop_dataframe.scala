package io.prophecy.pipelines.automationpbtno_v2scalatrue01.graph

import io.prophecy.libs._
import io.prophecy.pipelines.automationpbtno_v2scalatrue01.config.Context
import io.prophecy.pipelines.automationpbtno_v2scalatrue01.functions.UDFs._
import io.prophecy.pipelines.automationpbtno_v2scalatrue01.functions.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object noop_dataframe {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    print("Successfully Executed Son.")
    var out0=in0
    out0
  }

}
