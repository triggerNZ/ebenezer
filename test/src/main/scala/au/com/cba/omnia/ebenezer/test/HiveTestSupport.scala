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

import java.io.File

import org.apache.commons.io.FileUtils

import org.apache.hadoop.fs.Path

import com.google.common.io.Files

import com.twitter.scrooge.ThriftStruct

import scalaz.Scalaz._

import au.com.cba.omnia.omnitool.Result

import au.com.cba.omnia.thermometer.hive.HiveSupport

import au.com.cba.omnia.beeswax.Hive

/** Adds support functions for setting up tests using Hive. */
trait HiveTestSupport { self: HiveSupport =>
  /**
    * Creates hive table using a supplied column definition (DDL).
    *
    * WARNING: This method is not thread safe. If the same database or table is created at the same
    * time Hive handles it badly and throws an SQL integrity exception.
    *
    * @param database         Name of the database. Will be created if not found.
    * @param table            Name of table to create.
    * @param columns          A list of the columns formatted as `[(name, type)]`.
    * @param partitionColumns A list of the partition columns formatted as `[(name, type.)]`.
    *                         If empty unpartitioned table will be created.
    * @param source           Resource location of input data. This may be either a file or a
    *                         partitioned directory (autodetected).
    */
  def setupHiveTestTableDDL(
    database: String, table: String, columns: List[(String, String)],
    partitionColumns: List[(String, String)], source: String
  ): Hive[Unit] = {
    def colDef(cols: List[(String, String)]): String =
      cols.map { case (n, t) => s"$n $t" }.mkString(", ")

    val columnDefinitions = colDef(columns)

    val p = colDef(partitionColumns)
    val partitionedBy = partitionColumns match {
      case Nil => ""
      case _   => s"PARTITIONED BY ($p)"
    }

    for {
      dst <- Hive.result(setupFiles(source))
      _   <- Hive.createDatabase(database)
      _   <- Hive.queries(List(
               s"""
                 CREATE EXTERNAL TABLE $database.$table
                 (
                   $columnDefinitions
                 )
                 $partitionedBy
                 ROW FORMAT DELIMITED
                 FIELDS TERMINATED BY '|' location '$dst'
               """,
               s"USE $database",
               s"MSCK REPAIR TABLE $table"
             ))
    } yield ()
  }

   /**
     * Creates hive table using a supplied column definition as a thrift schema.
     *
     * WARNING: This method is not thread safe. If the same database or table is created at the same
     * time Hive handles it badly and throws an SQL integrity exception.
     *
     * @param database         Name of the database. Will be created if not found.
     * @param table            Name of table to create.
     * @param partitionColumns A list of the partition columns formatted as `[(name, type.)]`.
     *                         If empty unpartitioned table will be created.
     * @param source           Resource location of input data. This may be either a file or a
     *                         partitioned directory (autodetected).
     * @param delimiter        Delimiter used in input data. Defaults to `|`.
     */
  def setupHiveTestTable[T <: ThriftStruct : Manifest](
    database: String, table: String, partitionColumns: List[(String, String)], source: String, delimiter: String = "|"
  ): Hive[Unit] = for {
    dst <- Hive.result(setupFiles(source)).map(f => new Path(s"file://$f"))
    _   <- Hive.createTextTable(database, table, partitionColumns, Some(dst), delimiter)
    _   <- Hive.queries(List(s"USE $database", s"MSCK REPAIR TABLE $table"))
  } yield ()

  /** Copies a resources file or directory to a temporary location. */
  def setupFiles(source: String): Result[String] = Result.safe {
    val sourcePath = new File(getClass.getResource(source).getPath)
    val dst        = Files.createTempDir()

    if (sourcePath.isFile()) FileUtils.copyFileToDirectory(sourcePath, dst)
    else                     FileUtils.copyDirectory(sourcePath, dst)

    dst.toString
  }
}
