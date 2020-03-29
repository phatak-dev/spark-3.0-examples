package com.madhukaraphatak.spark.core.plugins.rpccommunication

import java.util

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}

case class RpcMessage(message:String) extends Serializable

class RpcSparkPlugin extends SparkPlugin{
  override def driverPlugin(): DriverPlugin = new DriverPlugin {
    override def receive(message: scala.Any): AnyRef = {
      message match
      {
        case RpcMessage(message) => {
          println(" executor sent the message " + message)
          RpcMessage(s"received $message in driver")
        }
        case _ => RpcMessage("unknown message sent")
      }
    }
  }

  override def executorPlugin(): ExecutorPlugin = new ExecutorPlugin {
    var pluginContext:PluginContext = null
    override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
      pluginContext = ctx
       val rpcMessage = new RpcMessage("this is a fire and forget message by executor")
      pluginContext.send(rpcMessage)
    }

    override def shutdown(): Unit = {
      val rpcMessage = new RpcMessage("this is blocking message from executor")
      val rpcResponse = pluginContext.ask(rpcMessage)
      println("the response for ask message is " + rpcResponse)
    }
  }
}
