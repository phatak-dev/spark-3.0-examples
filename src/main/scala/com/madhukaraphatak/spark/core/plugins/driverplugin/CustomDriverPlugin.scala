package com.madhukaraphatak.spark.core.plugins.driverplugin
import java.net.ServerSocket
import java.util

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, PluginContext}
import org.apache.spark.sql.SparkSession

class CustomDriverPlugin extends DriverPlugin{

  var sparkContext:SparkContext =null
  var runningThread:Thread = null

  class  ServerSocketListener {
    var port = 9999
    val listener = new ServerSocket(port)
    while (true) {
      val socket = listener.accept()
      new Thread(){
        override def run(): Unit = {
          println(" got client " + socket.getInetAddress)
          val sparkSession = SparkSession.builder().getOrCreate()
          sparkSession.catalog.uncacheTable("test")
        }
        socket.close()
      }.start()
    }
  }

  override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
    println("########### called init of custom driver plugin")
    this.sparkContext =sparkContext

    runningThread = new Thread(){
      override def run(): Unit = {
        new ServerSocketListener()
      }
    }
    runningThread.start()

    super.init(sc, pluginContext)
  }
  override def shutdown(): Unit = {
    println("############ called shutdown")
    runningThread.interrupt()
    System.exit(0)
    super.shutdown()
  }
}
