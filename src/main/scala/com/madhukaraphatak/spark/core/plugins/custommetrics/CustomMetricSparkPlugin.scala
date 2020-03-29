package com.madhukaraphatak.spark.core.plugins.custommetrics

import java.util

import com.codahale.metrics.Gauge
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}

class CustomMetricSparkPlugin extends SparkPlugin{
  override def driverPlugin(): DriverPlugin = new DriverPlugin {

    override def registerMetrics(appId: String, pluginContext: PluginContext): Unit = {
      val metricRegistry = pluginContext.metricRegistry()
      metricRegistry.register("exampleMetric", new Gauge[Long] {
        override def getValue: Long = 10
      })
    }
  }

  override def executorPlugin(): ExecutorPlugin = new ExecutorPlugin {
    var pluginContext:PluginContext = null
    override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
      pluginContext = ctx
    }

    override def shutdown(): Unit = {
      val metricRegistry = pluginContext.metricRegistry()
      metricRegistry.register("exampleMetric", new Gauge[Long] {
        override def getValue: Long = 10
      })
    }
  }
}
