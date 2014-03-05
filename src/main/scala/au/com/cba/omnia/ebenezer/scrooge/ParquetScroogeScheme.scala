package au.com.cba.omnia.ebenezer
package scrooge

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import parquet.cascading.ParquetValueScheme
import parquet.hadoop.ParquetInputFormat
import parquet.hadoop.mapred.DeprecatedParquetInputFormat
import parquet.hadoop.mapred.DeprecatedParquetOutputFormat
import parquet.hadoop.thrift.ParquetThriftInputFormat
import cascading.flow.FlowProcess
import cascading.tap.Tap

import com.twitter.scrooge.ThriftStruct

class ParquetScroogeScheme[A <: ThriftStruct](implicit manifest: Manifest[A]) extends ParquetValueScheme[A] {
  lazy val thrift: Class[A] = manifest.runtimeClass.asInstanceOf[Class[A]]

  def sinkConfInit(flow: FlowProcess[JobConf], tap: Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]], conf: JobConf): Unit = {
    conf.setOutputFormat(classOf[DeprecatedParquetOutputFormat[_]])
    DeprecatedParquetOutputFormat.setWriteSupportClass(conf, classOf[ScroogeWriteSupport[_]])
    ScroogeReadWriteSupport.setThriftClass[A](conf, thrift)
  }

  def sourceConfInit(flow: FlowProcess[JobConf], tap: Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]], conf: JobConf): Unit = {
    conf.setInputFormat(classOf[DeprecatedParquetInputFormat[_]])
    ParquetInputFormat.setReadSupportClass(conf, classOf[ScroogeReadSupport[_]])
    ScroogeReadWriteSupport.setThriftClass[A](conf, thrift)
  }
}
