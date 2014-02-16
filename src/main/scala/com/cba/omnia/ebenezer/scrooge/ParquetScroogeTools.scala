package com.cba.omnia.ebenezer
package scrooge

import com.twitter.scalding._
import com.twitter.scalding.TDsl._

import com.twitter.scrooge.ThriftStruct

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import parquet.hadoop.ParquetReader

object ParquetScroogeTools {
  def iteratorFromPath[A <: ThriftStruct](conf: Configuration, path: Path)(implicit m: Manifest[A]): Iterator[A] = {
    val cls = m.runtimeClass.asSubclass[ThriftStruct](classOf[ThriftStruct])
    ScroogeReadWriteSupport.setThriftClass(conf, cls)
    val reader = new ParquetReader[A](conf, path, new ScroogeReadSupport[A])
    new Iterator[A] {
      var state: A = reader.read
      def hasNext = state != null
      def next = { val record = state; state = reader.read; record }
    }
  }

  def streamFromPath[A <: ThriftStruct](conf: Configuration, path: Path)(implicit m: Manifest[A]): Stream[A] =
    iteratorFromPath(conf, path).toStream

  def listFromPath[A <: ThriftStruct](conf: Configuration, path: Path)(implicit m: Manifest[A]): List[A] =
    iteratorFromPath(conf, path).toList

}
