package com.madhukaraphatak.spark.core.plugins.rpccommunication

import java.util

import com.codahale.metrics.Counter
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}


case object InitialConfigRequest extends  Serializable
case class InitialConfigResponse(value:Int) extends Serializable

case class FinalValueResponse(value : Int) extends Serializable
case class RpcMessage(message:String) extends Serializable


class RpcSparkPlugin extends SparkPlugin{
  override def driverPlugin(): DriverPlugin = new DriverPlugin {
   override def receive(message: scala.Any): AnyRef = {
     message match {
       case InitialConfigRequest => InitialConfigResponse(10)
       case FinalValueResponse(value)  => println("the final value is "+ value); Unit

     }
 }
}

  override def executorPlugin(): ExecutorPlugin = new ExecutorPlugin {
    var pluginContext:PluginContext = null
    var initialConfiguration:Int = 0

    override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
      pluginContext = ctx
      initialConfiguration = pluginContext.ask(InitialConfigRequest).asInstanceOf[InitialConfigResponse].value
      println("the initial configuration is " + initialConfiguration)
    }

    override def shutdown(): Unit = {
      val rpcMessage = FinalValueResponse(10 * initialConfiguration)
      pluginContext.send(rpcMessage)
    }
  }
}
