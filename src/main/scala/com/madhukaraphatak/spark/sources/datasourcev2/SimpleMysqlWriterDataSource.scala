package com.madhukaraphatak.spark.sources.datasourcev2.simplemysqlwriter

import java.sql.DriverManager
import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/*
  * Default source should some kind of relation provider
  *
  */

class DefaultSource extends TableProvider{
  override def getTable(caseInsensitiveStringMap: CaseInsensitiveStringMap): Table = new MysqlTable
}


class MysqlTable extends SupportsWrite{

  private val tableSchema = new StructType().add("user", StringType)

  override def newWriteBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): WriteBuilder = new MysqlWriterBuilder

  override def name(): String = this.getClass.toString

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_WRITE,
    TableCapability.TRUNCATE).asJava
}

class MysqlWriterBuilder extends WriteBuilder{
  override def buildForBatch(): BatchWrite = new MysqlBatchWriter()
}

class MysqlBatchWriter extends BatchWrite{
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory = new
  MysqlDataWriterFactory

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}
}

class MysqlDataWriterFactory extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId:Long): DataWriter[InternalRow] = new MysqlWriter()
}



object WriteSucceeded extends WriterCommitMessage

class MysqlWriter extends DataWriter[InternalRow] {
  val url = "jdbc:mysql://localhost/test"
  val user = "root"
  val password = "abc123"
  val table ="userwrite"

  val connection = DriverManager.getConnection(url,user,password)
  val statement = "insert into userwrite (user) values (?)"
  val preparedStatement = connection.prepareStatement(statement)


  override def write(record: InternalRow): Unit = {
    val value = record.getString(0)
    preparedStatement.setString(1,value)
    preparedStatement.executeUpdate()
  }

  override def commit(): WriterCommitMessage = WriteSucceeded

  override def abort(): Unit = {}

  override def close(): Unit = {}
}








