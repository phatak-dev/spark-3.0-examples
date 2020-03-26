package com.madhukaraphatak.spark.sources.datasourcev2.simplecsv

import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

/*
  * Default source should some kind of relation provider
  */
class DefaultSource extends TableProvider{

    override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType =
    getTable(null,Array.empty[Transform],caseInsensitiveStringMap.asCaseSensitiveMap()).schema()

  override def getTable(structType: StructType, transforms: Array[Transform], map: util.Map[String, String]): Table ={
      val path = map.get("path")
      new CsvBatchTable(path)
    }

}

object SchemaUtils {
  def getSchema(path:String):StructType = {
    val sparkContext = SparkSession.builder.getOrCreate().sparkContext
    val firstLine = sparkContext.textFile(path).first()
    val columnNames = firstLine.split(",")
    val structFields = columnNames.map(value ⇒ StructField(value, StringType))
    StructType(structFields)
  }
}
/*
  Defines Read Support and Initial Schema
 */

class CsvBatchTable(path:String) extends Table with SupportsRead {
  override def name(): String = this.getClass.toString

  override def schema(): StructType = SchemaUtils.getSchema(path)

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new CsvScanBuilder(path)
}


/*
   Scan object with no mixins
 */
class CsvScanBuilder(path:String) extends ScanBuilder {
  override def build(): Scan = new CsvScan(path)
}


// simple class to organise the partition
case class CsvPartition(val partitionNumber:Int, path:String, header:Boolean=true) extends InputPartition


/*
    Batch Reading Support

    The schema is repeated here as it can change after column pruning etc
 */

class CsvScan(path:String) extends Scan with Batch{
  override def readSchema(): StructType = SchemaUtils.getSchema(path)

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val sparkContext = SparkSession.builder.getOrCreate().sparkContext
    val rdd = sparkContext.textFile(path)
    val partitions = ( 0 to rdd.partitions.length - 1).map(value => CsvPartition(value, path))
    partitions.toArray

 }
  override def createReaderFactory(): PartitionReaderFactory = new CsvPartitionReaderFactory()
}


// reader factory
class CsvPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = new
      CsvPartitionReader(partition.asInstanceOf[CsvPartition])
}


// parathion reader
class CsvPartitionReader(inputPartition: CsvPartition) extends PartitionReader[InternalRow] {

  var iterator: Iterator[String] = null

  @transient
  def next = {
    if (iterator == null) {
      val sparkContext = SparkSession.builder.getOrCreate().sparkContext
      val rdd = sparkContext.textFile(inputPartition.path)
      val filterRDD = if (inputPartition.header) {
        val firstLine = rdd.first
        rdd.filter(_ != firstLine)
      }
      else rdd
      val partition = filterRDD.partitions(inputPartition.partitionNumber)
      iterator = filterRDD.iterator(partition, org.apache.spark.TaskContext.get())
    }
    iterator.hasNext
  }

  def get = {
    val line = iterator.next()
    InternalRow.fromSeq(line.split(",").map(value => UTF8String.fromString(value)))
  }

 def close() = Unit

}




