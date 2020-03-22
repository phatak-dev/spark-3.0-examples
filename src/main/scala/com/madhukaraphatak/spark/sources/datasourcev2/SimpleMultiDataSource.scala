package com.madhukaraphatak.spark.sources.datasourcev2.simplemulti

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

/*
  * Default source should some kind of relation provider
  */
class DefaultSource extends TableProvider{

    override def getTable(options: CaseInsensitiveStringMap): Table = new SimpleBatchTable()
}


/*
  Defines Read Support and Initial Schema
 */

class SimpleBatchTable extends Table with SupportsRead {
  override def name(): String = this.getClass.toString

  override def schema(): StructType = StructType(Array(StructField("value", StringType)))

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new SimpleScanBuilder()
}


/*
   Scan object with no mixins
 */
class SimpleScanBuilder extends ScanBuilder {
  override def build(): Scan = new SimpleScan
}

/*
    Batch Reading Support

    The schema is repeated here as it can change after column pruning etc
 */

class SimpleScan extends Scan with Batch{
  override def readSchema(): StructType =  StructType(Array(StructField("value", StringType)))

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    Array(new SimplePartition(0,4),
      new SimplePartition(5,9))
  }
  override def createReaderFactory(): PartitionReaderFactory = new SimplePartitionReaderFactory()
}

// simple class to organise the partition
class SimplePartition(val start:Int, val end:Int) extends InputPartition

// reader factory
class SimplePartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = new
      SimplePartitionReader(partition.asInstanceOf[SimplePartition])
}


// parathion reader
class SimplePartitionReader(inputPartition: SimplePartition) extends PartitionReader[InternalRow] {

  val values = Array("1", "2", "3", "4", "5","6","7","8","9","10")

  var index = inputPartition.start

  def next = index <= inputPartition.end

  def get = {
    val stringValue = values(index)
    val stringUtf = UTF8String.fromString(stringValue)
    val row = InternalRow(stringUtf)
    index = index + 1
    row
  }

  def close() = Unit

}




