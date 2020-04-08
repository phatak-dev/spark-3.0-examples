package com.madhukaraphatak.spark.core.plugins.dynamicconfig

import java.io.PrintWriter
import java.net.ServerSocket
import java.util

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, PluginContext}


class CustomConfigDriverPlugin extends DriverPlugin {

  var sparkContext: SparkContext = null
  var runningThread: Thread = null

  class ServerSocketListener {
    var port = 9999
    val listener = new ServerSocket(port)
    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run(): Unit = {
          val currentValue = Configuration.getConfig
          Configuration.changeConfig(currentValue + 10)
          val response = "HTTP/1.1 200 OK \r\n\r\n "+s" the latest configuration is ${Configuration.getConfig}"
          socket.getOutputStream().write(response.getBytes("UTF-8"))
          socket.getOutputStream.flush()
          socket.close()
        }
      }.start()
    }
  }

  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    this.sparkContext = sparkContext

    runningThread = new Thread() {
      override def run(): Unit = {
        new ServerSocketListener()
      }
    }
    runningThread.start()

    super.init(sc, pluginContext)
  }

  override def shutdown(): Unit = {
    runningThread.interrupt()
    System.exit(0)
    super.shutdown()
  }
}
