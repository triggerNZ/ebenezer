//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.ebenezer.test

import scalaz.Scalaz._

import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec

import au.com.cba.omnia.beeswax.Hive

object HiveTestSupportSpec
  extends ThermometerHiveSpec
  with HiveTestSupport
  with HiveMatchers { def is = s2"""

Set up an existing Hive Table
=============================

  Single data file test, with columns given as DDL          $ddlFilePipeline
  Partitioned data test, with columns given as DDL          $ddlPartitionedPipeline

  Single data file test, with columns given as Thrift       $thriftFilePipeline
  Partitioned data test, with columns given as Thrift       $thriftPartitionedPipeline

  Default delimiter test, with columns given as Thrift      $thriftFileDefaultDelimitedPipeline
  User defined delimiter test, with columns given as Thrift $thriftFileDelimitedPipeline

"""

  def ddlFilePipeline = {
    val result = for {
      _ <- setupHiveTestTableDDL(
             "testdb", "testtable",
             List(("id", "INT"), ("name", "STRING")),
             List(),
             "/hive-test/testdata"
           )
      n <- Hive.query("SELECT COUNT(*) FROM testdb.testtable")
    } yield n

    result must beValue(List("4"))
  }

  def ddlPartitionedPipeline = {
    val result = for {
      _ <- setupHiveTestTableDDL(
             "testdb2", "testtable",
             List(("id", "INT"), ("name", "STRING")),
             List(("year", "STRING"), ("month", "STRING"), ("day", "STRING")),
             "/hive-test/partitions/"
           )
      n <- Hive.query("SELECT COUNT(*) FROM testdb2.testtable")
    } yield n

    result must beValue(List("4"))
  }

  def thriftFilePipeline = {
    val result = for {
      _ <- setupHiveTestTable[TestHive](
             "testdb3", "testtable",
             List(),
             "/hive-test/testdata"
           )
      n <- Hive.query("SELECT COUNT(*) FROM testdb3.testtable")
    } yield n

    result must beValue(List("4"))
  }

  def thriftPartitionedPipeline = {
    val result = for {
      _ <- setupHiveTestTable[TestHive](
             "testdb4", "testtable",
             List(("year", "string"), ("month", "string"), ("day", "string")),
             "/hive-test/partitions/"
           )
      n <- Hive.query("SELECT COUNT(*) FROM testdb4.testtable")
    } yield n

    result must beValue(List("4"))
  }

  def thriftFileDefaultDelimitedPipeline = {
    val result = for {
      _ <- setupHiveTestTable[TestHive]("testdb5", "testtable", List(), "/hive-test/testdata")
      n <- Hive.query("SELECT name FROM testdb5.testtable")
    } yield n

    result must beValue(List("red", "green", "yellow", "orange"))
  }

  def thriftFileDelimitedPipeline = {
    val result = for {
      _ <- setupHiveTestTable[TestHive]("testdb6", "testtable", List(), "/hive-test/testdata1", ",")
      n <- Hive.query("SELECT name FROM testdb6.testtable")
    } yield n

    result must beValue(List("red", "green", "yellow", "orange"))
  }
}
