package com.cba.omnia.ebenezer
package test

import com.cba.omnia.ebenezer.scrooge._
import com.cba.omnia.thermometer.core._, Thermometer._

import com.twitter.scrooge._

import org.apache.hadoop.mapred.JobConf

import scalaz.effect.IO

object ParquetThermometerRecordReader {
  def apply[A <: ThriftStruct : Manifest] =
    ThermometerRecordReader((conf, path) => IO {
      ParquetScroogeTools.listFromPath[A](conf, path) })
}
