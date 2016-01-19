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

import org.apache.hadoop.fs.Path

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.IMetaStoreClient

import com.twitter.scalding.Execution
import com.twitter.scalding.typed.IterablePipe

import scalaz.Scalaz._
import scalaz.Equal
import scalaz.\&/.This

import org.specs2.matcher.Matcher
import org.specs2.execute.{Result => SpecResult}
import org.specs2.scalacheck.Parameters

import org.scalacheck.Arbitrary

import au.com.cba.omnia.omnitool.{Result, Ok, Error}
import au.com.cba.omnia.omnitool.test.OmnitoolProperties.resultantMonad
import au.com.cba.omnia.omnitool.test.Arbitraries._

import au.com.cba.omnia.thermometer.core.Thermometer
import au.com.cba.omnia.thermometer.hive.ThermometerHiveSpec

import au.com.cba.omnia.beeswax.{Hive, ParquetFormat}

import au.com.cba.omnia.ebenezer.scrooge.PartitionParquetScroogeSink
import au.com.cba.omnia.ebenezer.ParquetLogging

object HiveSpec extends ThermometerHiveSpec with ParquetLogging { def is = sequential ^ s2"""
Hive Operations
===============

  can add new partitions to a hive table                                     $addPartitions
  can add an existing partition again to a hive table                        $addExitingPartition

"""

  implicit val params = Parameters(minTestsOk = 10)

  def safeHive = prop { (t: Throwable) =>
    Hive.withConf(_ => throw t)     must beResult { Result.exception(t) }
    Hive.withClient(_ => throw t)   must beResult { Result.exception(t) }
    Hive.value(3).map(_ => throw t) must beResult { Result.exception(t) }
  }

  def addPartitions = {
    val database  = "comic"
    val table     = "person"
    val tablePath = s"file:$hiveWarehouse/comic.db/person"
    val data      = List(
      Person("Bruce", "Wayne", 40),
      Person("Alfred", "Pennyworth", 61),
      Person("Jane", "Pennyworth", 23)
    )

    val exec = Execution.from(
      (for {
        _    <- Hive.createParquetTable[Person](database, table, List("plastname" -> "string"))
        path <- Hive.getPath(database, table)
      } yield path.toString).run(hiveConf).toOption.get
    ).flatMap { p =>
      IterablePipe(data).map(c => c.lastname -> c)
        .writeExecution(PartitionParquetScroogeSink[String, Person]("plastname=%s", p))
    }.flatMap(_ => Execution.from(
      (for {
        path  <- Hive.getPath(database, table)
        _     <- Hive.addPartitions(database, table, List("plastname"), 
                   List(new Path(path, "plastname=Wayne"), new Path(path, "plastname=Pennyworth"))
                 )
        parts <- Hive.listPartitions(database, table)
        recs  <- Hive.query(s"SELECT firstname from $database.$table")
      } yield (parts, recs)).run(hiveConf)
    ))

    executesSuccessfully(exec) must_== Ok((
      List(new Path(s"$tablePath/plastname=Pennyworth"), new Path(s"$tablePath/plastname=Wayne")),
      List("Alfred", "Jane", "Bruce")
    ))
  }

  def addExitingPartition = {
    val database  = "comic"
    val table     = "person"
    val tablePath = s"file:$hiveWarehouse/comic.db/person"

    val x = for {
      _     <- Hive.createParquetTable[Person](database, table, List("plastname" -> "string"))
      path  <- Hive.getPath(database, table)
      _     <- Hive.addPartitions(database, table, List("plastname"), 
                 List(new Path(path, "plastname=Wayne"), new Path(path, "plastname=Pennyworth"))
               )
      _     <- Hive.addPartitions(database, table, List("plastname"), 
                 List(new Path(path, "plastname=Wayne"), new Path(path, "plastname=Pennyworth"))
               )
      parts <- Hive.listPartitions(database, table)
    } yield parts

    x must beValue(List(new Path(s"$tablePath/plastname=Pennyworth"), new Path(s"$tablePath/plastname=Wayne")))
  }

  def query = {
    val x = for {
      _   <- Hive.createDatabase("test")
      dbs <- Hive.query("SHOW DATABASES")
    } yield dbs

    x must beValue(List("test"))
  }

  def queries = {
    val x = for {
      _   <- Hive.createTextTable[SimpleHive]("test", "test2", List("part1" -> "string", "part2" -> "string"), None)
      res <- Hive.queries(List("SHOW DATABASES", "SHOW TABLES IN test"))
    } yield res

    x must beValue(List(List("test"), List("test2")))
  }

  def queriesOrdered = {
    val x = for {
      _   <- Hive.createTextTable[SimpleHive]("test", "test2", List("part1" -> "string", "part2" -> "string"), None)
      _   <- Hive.queries(List("USE test", "SHOW TABLES"))
    } yield ()

    x must beValue(())
  }

  def queryError = {
    val x = for {
      _   <- Hive.createDatabase("test")
      dbs <- Hive.query("SHOW DATABS")
    } yield dbs

    x.run(hiveConf) must beLike {
      case Error(_) => ok
    }
  }

  def hiveParquetMatch = {
    val db    = "test"
    val table = "test"
    // DDL needs to match SimpleHive plus partition columns
    val ddl = s"""
      CREATE TABLE $db.$table (
        stringfield string
      ) PARTITIONED BY (part string)
      STORED AS PARQUET
      """

    val x = for {
      _ <- Hive.createDatabase(db)
      _ <- Hive.query(ddl)
      t <- Hive.existsTableStrict[SimpleHive](db, table, List("part" -> "string"), None, ParquetFormat)
    } yield t

    x must beValue(true)
  }

  /** Note these are not general purpose, specific to testing laws. */
  implicit def HiveArbirary[A : Arbitrary]: Arbitrary[Hive[A]] =
    Arbitrary(Arbitrary.arbitrary[Result[A]] map (Hive.result(_)))

  implicit def HiveEqual: Equal[Hive[Int]] =
    Equal.equal[Hive[Int]]((a, b) =>
      a.run(hiveConf) must_== b.run(hiveConf))

  def beResult[A](expected: Result[A]): Matcher[Hive[A]] =
    (h: Hive[A]) => h.run(hiveConf) must_== expected

  def beResultLike[A](expected: Result[A] => SpecResult): Matcher[Hive[A]] =
    (h: Hive[A]) => expected(h.run(hiveConf))

  def beValue[A](expected: A): Matcher[Hive[A]] =
    beResult(Result.ok(expected))
}
