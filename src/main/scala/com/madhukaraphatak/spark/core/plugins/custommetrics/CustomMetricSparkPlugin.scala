package com.madhukaraphatak.spark.core.plugins.custommetrics

import java.util
import java.util.concurrent.atomic.AtomicLong

import com.codahale.metrics.Gauge
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}

object CustomMetricSparkPlugin {
  val value = new AtomicLong()
}

class CustomMetricSparkPlugin extends SparkPlugin{

  override def driverPlugin(): DriverPlugin = null
  override def executorPlugin(): ExecutorPlugin = new ExecutorPlugin {
   override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
      val metricRegistry = ctx.metricRegistry()
      metricRegistry.register("evenMetrics", new Gauge[Long] {
        override def getValue: Long = CustomMetricSparkPlugin.value.get()
      })
    }
  }
}
