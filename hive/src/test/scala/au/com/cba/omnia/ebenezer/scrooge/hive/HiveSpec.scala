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

package au.com.cba.omnia.ebenezer.scrooge.hive

import org.apache.hadoop.fs.Path

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.IMetaStoreClient

import scalaz.Equal

import org.specs2.matcher.{Matcher, Parameters}
import org.specs2.execute.{Result => SpecResult}

import org.scalacheck.Arbitrary

import au.com.cba.omnia.omnitool.{Result, Ok, Error}
import au.com.cba.omnia.omnitool.test.OmnitoolProperties.resultantMonad
import au.com.cba.omnia.omnitool.test.Arbitraries._

import au.com.cba.omnia.thermometer.core.{Thermometer, ThermometerSpec}, Thermometer._
import au.com.cba.omnia.thermometer.hive.HiveSupport

object HiveSpec extends ThermometerSpec with HiveSupport { def is = s2"""
Hive Operations
===============

Hive operations should:
  obey resultant monad laws (monad and plus laws)                            ${resultantMonad.laws[Hive]}

Hive operations:
  hive handles exceptions                                                    $safeHive
  tableExists should always be false                                         $noTable
  created table must exist                                                   $create
  creating table with different schema fails                                 $create2
  can verify schema                                                          $strict
  can getPath for managed table                                              $pathManaged
  can getPath for unmanaged table                                            $pathUnmanaged
  query                                                                      $query
  queries                                                                    $queries
  queries must be run in order                                               $queriesOrdered
  query catches errors                                                       $queryError
  Hive existsTableStrict should accept parquet tables created using Hive DDL $hiveParquetMatch

"""

  implicit val params = Parameters(minTestsOk = 10)

  def safeHive = prop { (t: Throwable) =>
    Hive.withConf(_ => throw t)     must beResult { Result.exception(t) }
    Hive.withClient(_ => throw t)   must beResult { Result.exception(t) }
    Hive.value(3).map(_ => throw t) must beResult { Result.exception(t) }
  }
  
  def noTable = {
    Hive.existsTable("test", "test")                               must beValue(false)
    Hive.existsTableStrict[SimpleHive]("test", "test", List.empty) must beValue(false)
  }

  def create = {
    val x = for {
      t1 <- Hive.createParquetTable[SimpleHive]("test", "test", List("part1" -> "string", "part2" -> "string"), None)
      t2 <- Hive.createParquetTable[SimpleHive]("test", "test", List("part1" -> "string", "part2" -> "string"), None)
      t3 <- Hive.existsTable("test", "test")
      t4 <- Hive.existsTableStrict[SimpleHive]("test", "test", List("part1" -> "string", "part2" -> "string"), None)
    } yield (t1, t2, t3, t4)
    
    x must beValue((true, false, true, true))
  }

  def create2 = {
    val x = for {
      t1 <- Hive.createParquetTable[SimpleHive]("test", "test", List("part1" -> "string", "part2" -> "string"), None)
      t2 <- Hive.createParquetTable[SimpleHive]("test", "test", List("part1" -> "string", "part3" -> "string"), None)
    } yield (t1, t2)
    
    x.run(hiveConf) must beLike {
      case Error(_) => ok
    }
  }

  def strict = {
    val x = for {
      _  <- Hive.createParquetTable[SimpleHive]("test", "test", List("part1" -> "string", "part2" -> "string"), None)
      t1 <- Hive.existsTable("test", "test")
      t2 <- Hive.existsTableStrict[SimpleHive]("test", "test", List("part1" -> "string"), None)
      t3 <- Hive.existsTableStrict[SimpleHive]("test", "test", List("part1" -> "string", "part2" -> "string"), Some(new Path("/other")))
      t4 <- Hive.existsTableStrict[SimpleHive]("test", "test", List("part1" -> "string", "part2" -> "string"), None, TextFormat())
      t5 <- Hive.existsTableStrict[SimpleHive2]("test", "test", List("part1" -> "string", "part2" -> "string"), None)
    } yield (t1, t2, t3, t4, t5)
    
    x must beValue((true, false, false, false, false))
  }

  def pathManaged = {
    val x = for {
      _    <- Hive.createParquetTable[SimpleHive]("test", "test", List("part1" -> "string", "part2" -> "string"), None)
      path <- Hive.getPath("test", "test")
    } yield path

    x must beValue(new Path(s"file:$hiveWarehouse/test.db/test"))
  }

  def pathUnmanaged = {
    val x = for {
      _    <- Hive.createParquetTable[SimpleHive]("test", "test", List("part1" -> "string", "part2" -> "string"), Some(new Path("test")))
      path <- Hive.getPath("test", "test")
    } yield path

    x must beValue(new Path(s"file:$dir/user/test"))
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
