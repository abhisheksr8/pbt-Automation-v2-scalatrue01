package io.prophecy.pipelines.automationpbt_v2scalatrue01.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
