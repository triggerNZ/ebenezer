package au.com.cba.omnia.ebenezer.example

import com.twitter.scalding._

import au.com.cba.omnia.thermometer.core._, Thermometer._
import au.com.cba.omnia.thermometer.fact.PathFactoids._
import au.com.cba.omnia.thermometer.hive.HiveSupport

import au.com.cba.omnia.ebenezer.test.ParquetThermometerRecordReader

object ExampleSpec extends ThermometerSpec with HiveSupport { def is = sequential ^ s2"""
  Examples
  ========

  Example 1 runs $example1
  Example 2 doesn't run since it depends on an existing table
  Example 3 runs $example3
  Example 4 runs $example4
"""

  def example1 = {
    val job = withArgs(Map("db" -> "default", "table" -> "dst"))(new HiveExampleStep1(_))
    val facts = job.data.groupBy(_.id).map { case (k, vs) => 
      hiveWarehouse </> "dst" </> s"pid=$k" </> "*.parquet" ==> records(ParquetThermometerRecordReader[Customer], vs)
    }.toList

    job.withFacts(facts: _*)
  }

  def example3 = {
    val job = withArgs(Map("db" -> "default", "src-table" -> "src", "dst-table" -> "dst"))(new HiveExampleStep3(_))
    val facts = (hiveWarehouse </> "test" </> "0*" ==> count(4)) +: job.data.flatMap(c => List(
      hiveWarehouse </> "src" </> s"pid=${c.id}" </> "*.parquet" ==> recordCount(ParquetThermometerRecordReader[Customer], 1),
      hiveWarehouse </> "dst" </> s"pid=${c.id}" </> "0*"        ==> recordCount(ParquetThermometerRecordReader[Customer], 1)
      )) 

    job.withFacts(facts: _*)
  }

  def example4 = {
    val job = withArgs(Map("db" -> "default", "table" -> "dst"))(new HiveExampleStep4(_))
    val fact =
      hiveWarehouse </> "dst" </> "*.parquet" ==> records(ParquetThermometerRecordReader[Nested], job.data)

    job.withFacts(fact)
  }
}
