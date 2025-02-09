package io.prophecy.pipelines.automationpbt_v2scalatrue01

import io.prophecy.libs._
import io.prophecy.pipelines.automationpbt_v2scalatrue01.config._
import io.prophecy.pipelines.automationpbt_v2scalatrue01.functions.UDFs._
import io.prophecy.pipelines.automationpbt_v2scalatrue01.functions.PipelineInitCode._
import io.prophecy.pipelines.automationpbt_v2scalatrue01.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_s3_source_dataset = s3_source_dataset(context)
    create_lookup_test(context, df_s3_source_dataset)
    val df_reformat_customer_data =
      reformat_customer_data(context, df_s3_source_dataset)
    val df_noop_dataframe = noop_dataframe(context, df_reformat_customer_data)
    val df_select_from_temp_view =
      select_from_temp_view(context, df_s3_source_dataset)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("AutomationPBT_v2-scalatrue01")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/AutomationPBT_v2-scalatrue01"
    )
    spark.conf.set("spark.default.parallelism",             "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/AutomationPBT_v2-scalatrue01"
    ) {
      apply(context)
    }
  }

}
