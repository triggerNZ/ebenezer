package au.com.cba.omnia.ebenezer
package test

import au.com.cba.omnia.ebenezer.scrooge._
import au.com.cba.omnia.thermometer.core._, Thermometer._

import com.twitter.scrooge._

import org.apache.hadoop.mapred.JobConf

import scalaz.effect.IO

object ParquetThermometerRecordReader {
  def apply[A <: ThriftStruct : Manifest] =
    ThermometerRecordReader((conf, path) => IO {
      ParquetScroogeTools.listFromPath[A](conf, path) })
}
