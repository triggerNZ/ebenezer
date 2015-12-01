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

import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.util.matching.Regex
import scala.collection.mutable.{Map => MMap}

import java.util.ArrayList

import scalaz._, Scalaz._

import com.twitter.scrooge.ThriftStruct

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.{HiveMetaHookLoader, HiveMetaStoreClient, IMetaStoreClient, RetryingMetaStoreClient}
import org.apache.hadoop.hive.metastore.api.{Database, Table, Partition, StorageDescriptor, AlreadyExistsException, NoSuchObjectException}
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TMemoryBuffer


import au.com.cba.omnia.omnitool.{Result, ResultantMonad, ResultantOps, ToResultantMonadOps}

/**
  * A data-type that represents a Hive operation.
  *
  * Hive operations use a HiveConf as context, and produce a (potentially failing) result. For
  * convenience Hive operations receive both a HiveConf and a handle to a Hive client. The client
  * is created from the HiveConf when the run method is called.
  * 
  */
// NB that this is the Hive equivalent of the HDFS monad in permafrost.
case class Hive[A](action: (HiveConf, IMetaStoreClient) => Result[A]) {
  /** Runs the Hive action with a RetryingMetaStoreClient created based on the provided HiveConf. */
  def run(hiveConf: HiveConf): Result[A] = {
    try {
      val client = RetryingMetaStoreClient.getProxy(
        hiveConf,
        new HiveMetaHookLoader() {
          override def getHook(tbl: Table) = null
        },
        classOf[HiveMetaStoreClient].getName()
      )

      try {
        val result = action(hiveConf, client)
        result
      } catch {
        case NonFatal(t) => Result.error("Failed to run hive operation", t)
      } finally {
        client.close
      }
    } catch {
      case NonFatal(t) => Result.error("Failed to create client", t)
    }
  }
}

/** Hive operations */
// NB that this is the Hive equivalent of the HDFS monad in permafrost.
object Hive extends ResultantOps[Hive] with ToResultantMonadOps {
  /** Gets the Hive conf. */
  def getConf: Hive[HiveConf] =
    Hive((conf, _) => Result.ok(conf))

  /** Gets the Hive client. */
  def getClient: Hive[IMetaStoreClient] =
    Hive((_, client) => Result.ok(client))

  /** Gets the Hive conf and client. */
  def getConfClient: Hive[(HiveConf, IMetaStoreClient)] =
    Hive((conf, client) => Result.ok((conf, client)))

  /** Builds a Hive operation from a function. The resultant Hive operation will not throw an exception. */
  def withConf[A](f: HiveConf => A): Hive[A] =
    Hive((conf, _) => Result.safe(f(conf)))

  /** Builds a Hive operation from a function. The resultant Hive operation will not throw an exception. */
  def withClient[A](f: IMetaStoreClient => A): Hive[A] =
    Hive((_, client) => Result.safe(f(client)))

  /**
    * Creates a database if it doesn't already exists. Returns false if the DB already exists.
    * 
    * WARNING: This method is not thread safe. If the same database or table is created at the same
    * time Hive handles it badly and throws an SQL integrity exception.
    */
  def createDatabase(
    database: String, description: String = "", location: Option[Path] = None, parameters: Map[String, String] = Map.empty
  ): Hive[Boolean] =
    Hive.existsDatabase(database) >>= (exists =>
      if (exists) Hive.value(false)
      else {
        val db = new Database(database, description, location.cata(_.toString, null), parameters.asJava)
        Hive((_, client) => try {
          client.createDatabase(db)
          Result.ok(true)
        } catch {
          case _: AlreadyExistsException => Result.ok(false)
          case NonFatal(t)               => Result.error(s"Failed to create database $database", t)
        })
      }
    )
  
  /** Checks if the specified database exists. */
  def existsDatabase(database: String): Hive[Boolean] =
    Hive.withClient(!_.getDatabases(database).isEmpty)
      .addMessage(s"Failed to check if $database exists.")

  /**
    * Creates hive table with the specified hive storage format.
    * 
    * Returns true if the table was created. If the table with the same schema already exists it
    * returns false. If a table with the same name but different schema exists it returns a Hive error.
    *
    * WARNING: This method is not thread safe. If the same database or table is created at the same
    * time Hive handles it badly and throws an SQL integrity exception.
    *
    * @param database Name of the database. Will be created if not found.
    * @param table Name of table to create.
    * @param partitionColumns A list of the partition columns formatted as `[(name, type.)]`.
    *                         If empty unpartitioned table will be created.
    * @param location Optional location for the hive table. Not set by default.
    * @param format Storage format of the hive table.
    */
  def createTable[T <: ThriftStruct : Manifest](
    database: String, table: String, partitionColumns: List[(String, String)],
    location: Option[Path] = None, format: HiveStorageFormat
  ): Hive[Boolean] = {
    Hive.createDatabase(database)     >>
    Hive.existsTable(database, table) >>= (exists =>
      if (exists)
        Hive.mandatory(
          Hive.existsTableStrict[T](database, table, partitionColumns, location, format),
          s"$database.$table already exists but has different schema."
        ).map(_ => false)
      else
        Hive.getConfClient >>= { case (conf, client) =>
          val fqLocation = location.map(FileSystem.get(conf).makeQualified(_))
          val metadataTable = HiveMetadataTable[T](database, table, partitionColumns, format, fqLocation)

          try {
            client.createTable(metadataTable)
            Hive.value(true)
          } catch {
            case _: AlreadyExistsException =>
              Hive.mandatory(
                existsTableStrict[T](database, table, partitionColumns, location, format),
                s"$database.$table already exists but has different schema."
              ).map(_ => false)
            case NonFatal(t)               => Hive.error(s"Failed to create table $database.$table", t)
          }
        }
    )
  }

  /**
    * Creates hive text table
    *
    * WARNING: This method is not thread safe. If the same database or table is created at the same
    * time Hive handles it badly and throws an SQL integrity exception.
    *
    * @param database Name of the database. Will be created if not found.
    * @param table Name of table to create.
    * @param partitionColumns A list of the partition columns formatted as `[(name, type.)]`.
    *                         If empty unpartitioned table will be created.
    * @param location Optional location for the hive table. Not set by default.
    * @returns true if the table already exists false otherwise
    */
  def createTextTable[T <: ThriftStruct : Manifest](
    database: String, table: String, partitionColumns: List[(String, String)],
    location: Option[Path] = None, delimiter: String = TextFormat.DEFAULT_DELIMITER
  ): Hive[Boolean] = createTable(database, table, partitionColumns, location, TextFormat(delimiter))

  /**
    * Creates hive parquet table
    *
    * WARNING: This method is not thread safe. If the same database or table is created at the same
    * time Hive handles it badly and throws an SQL integrity exception.
    *
    * @param database Name of the database. Will be created if not found.
    * @param table Name of table to create.
    * @param partitionColumns A list of the partition columns formatted as `[(name, type.)]`.
    *                         If empty unpartitioned table will be created.
    * @param location Optional location for the hive table. Not set by default.
    * @returns true if the table already exists false otherwise
    */
  def createParquetTable[T <: ThriftStruct : Manifest](
    database: String, table: String, partitionColumns: List[(String, String)],
    location: Option[Path] = None
  ): Hive[Boolean] = createTable(database, table, partitionColumns, location, ParquetFormat)

  /** Checks if the named table exists. */
  def existsTable(database: String, table: String): Hive[Boolean] =
    withClient(_.tableExists(database, table))

  /** Checks if a table with the same name and schema already exists. */
  def existsTableStrict[T <: ThriftStruct : Manifest](
    database: String, table: String, partitionColumns: List[(String, String)],
    location: Option[Path] = None, format: HiveStorageFormat = ParquetFormat
  ): Hive[Boolean] = Hive((conf, client) => try {
    val fs               = FileSystem.get(conf)
    val actualTable      = client.getTable(database, table)
    val expectedTable    = HiveMetadataTable[T](database, table, partitionColumns, format, location.map(fs.makeQualified(_)))
    val actualCols       = actualTable.getSd.getCols.asScala.map(c => (c.getName.toLowerCase, c.getType.toLowerCase)).toList
    val expectedCols     = expectedTable.getSd.getCols.asScala.map(c => (c.getName.toLowerCase, c.getType.toLowerCase)).toList
    //partition keys of unpartitioned table comes back from the metastore as an empty list
    val actualPartCols   = actualTable.getPartitionKeys.asScala.map(pc => (pc.getName.toLowerCase, pc.getType.toLowerCase)).toList
    //partition keys of unpartitioned table not submitted to the metastore will be null
    val expectedPartCols = Option(expectedTable.getPartitionKeys).map(_.asScala.map(
      pc => (pc.getName.toLowerCase, pc.getType.toLowerCase)).toList
    ).getOrElse(List.empty)
    val warehouse        = conf.getVar(ConfVars.METASTOREWAREHOUSE)
    val actualPath       = fs.makeQualified(new Path(actualTable.getSd.getLocation))
    //Do we want to handle `default` database separately
    val expectedLocation = Option(expectedTable.getSd.getLocation).getOrElse(
      s"$warehouse/${expectedTable.getDbName}.db/${expectedTable.getTableName}"
    )
    val expectedPath     = fs.makeQualified(new Path(expectedLocation))

    val delimiterComparison = format match {
      case ParquetFormat => true
      case TextFormat(delimiter) =>
        actualTable.getSd.getSerdeInfo.getParameters.asScala.get("field.delim").exists(_ == delimiter)
    }

    Result.ok(
      actualTable.getTableType          == expectedTable.getTableType          &&
      actualPath                        == expectedPath                        &&
      actualCols                        == expectedCols                        &&
      actualPartCols                    == expectedPartCols                    &&
      actualTable.getSd.getInputFormat  == expectedTable.getSd.getInputFormat  &&
      actualTable.getSd.getOutputFormat == expectedTable.getSd.getOutputFormat &&
      delimiterComparison                    
    )
  } catch {
    case _: NoSuchObjectException => Result.ok(false)
    case NonFatal(t)              => Result.error(s"Failed to check strict existence of $database.$table", t)
  })

  /** Gets the on disk location of a Hive table. */
  def getPath(database: String, table: String): Hive[Path] = Hive((conf, client) =>
    try {
      val location = client.getTable(database, table).getSd.getLocation
      Result.ok(FileSystem.get(conf).makeQualified(new Path(location)))
    } catch {
      case _: NoSuchObjectException => Result.fail(s"Table $database.$table does not exist")
      case NonFatal(t)              => Result.error(s"Failed to get path for $database.$table", t)
    }
  )

  /** Runs the specified Hive query. Returns at most `maxRows` */
  def query(query: String, maxRows: Int = 100): Hive[List[String]] = Hive { (conf, _) =>
    SessionState.start(conf)
    SessionState.get().setIsSilent(true)
    val driver = new Driver(conf)
    try {
      driver.init()
      driver.setMaxRows(maxRows)
      val response = driver.run(query)
      if (response.getResponseCode() != 0)
        Result.fail(s"Error running query '$query'. ${response.getErrorMessage}")
      else {
        val results    = new ArrayList[String]()
        val gotResults = driver.getResults(results)
        if (gotResults) Result.ok(results.asScala.toList)
        else            Result.ok(List.empty[String])
      }
    } catch {
      case NonFatal(ex) => Result.error(s"Error trying to run query '$query'", ex)
    } finally {
      driver.destroy()
    }
  }

  /** Runs the specified Hive queries. Returns at most `maxRows` per query */
  def queries(queries: List[String], maxRows: Int = 100): Hive[List[List[String]]] = {
    val setup = Hive.getConf.flatMap(conf => Hive.value {
      SessionState.start(conf)
      SessionState.get().setIsSilent(true)
      val driver = new Driver(conf)
      driver.init()
      driver.setMaxRows(maxRows)
      driver
    })

    val cleanup = (driver: Driver) => Hive.value(driver.destroy)

    val body = (driver: Driver) => queries.traverse(query => {
      val runQuery = for {
        response <- Hive.value(driver.run(query))
        _        <- Hive.guard(response.getResponseCode == 0, response.getErrorMessage)
        results  <- Hive.value {
          val results    = new ArrayList[String]()
          val gotResults = driver.getResults(results)
          if (gotResults) results.asScala.toList
          else            List.empty[String]
        }
      } yield results

      runQuery.addMessage(s"Error trying to run query '$query'")
    })

    for {
      driver  <- setup
      results <- body(driver) ensure cleanup(driver)
    } yield results
  }

  /** List all partitions for a hive table
    *
    * @param database Name of the database. 
    * @param table Name of table to create.
    * @param maxSize Maximum number of partitions to be fetched. Defaulted to -1 which means there is no limit.
    * @returns List of absolute hdfs partition paths contained in the hive table.
    */
  def listPartitions(database: String, table: String, maxSize: Short = -1): Hive[List[Path]] = 
    for {
      tPath <- Hive.getPath(database, table)
      parts <- Hive.withClient(_.listPartitionNames(database, table, maxSize).asScala)
    } yield parts.map(new Path(tPath, _)).toList

  /** Add partitions to a hive table
    *
    * Please be aware that this implementation assumes that the Hive table is either a MANAGED_TABLE
    * or an EXTERNAL_TABLE. It doesn't accomodate for VIRTUAL_VIEW or INDEX_TABLE.
    *
    * WARNING: This method is not thread safe. 
    *
    * @param database Name of the database. 
    * @param table Name of table to create.
    * @param partitionColumnNames A list of the partition columns names.
    * @param paths A list of the absolute hdfs partition paths that needs to be added as partitions.
    */
  def addPartitions(
    database: String, table: String,
    partitionColumnNames: List[String], paths: List[Path]
  ): Hive[Unit] = {
    def pattern(cols: List[String]) = ("^" + cols.map(_ + "=([^/]+)").mkString("/") +"$").r

    def partition(t: Table, partitionPattern: Regex, partitionPath: String): Result[Partition] = {
      val partitionValues = partitionPattern.findAllMatchIn(partitionPath).map(_.subgroups).toList.flatten
      if (partitionValues.isEmpty) 
        Result.fail[Partition](s"""$partitionPath does not match table partitions - $partitionColumnNames""")
      else 
        Result.safe {
          val now = (System.currentTimeMillis() / 1000).toInt
          val nsd = t.getSd.deepCopy
          Option(t.getSd.getLocation).fold(())(loc => nsd.setLocation(s"$loc/$partitionPath"))
          new Partition(
            partitionValues.asJava, t.getDbName(), t.getTableName(), now, 
            now, nsd, MMap.empty[String, String].asJava
          )
        }
    }

    for {
      _               <- Hive.mandatory(Hive.existsTable(database, table), s"$database.$table does not exist.")
      mTable          <- Hive.withClient(_.getTable(database, table))
      _               <- Hive.guard(
                           mTable.getPartitionKeys.asScala.map(c => c.getName.toLowerCase) == 
                             partitionColumnNames.map(_.toLowerCase),
                           s"""$database.$table does not have partitions - $partitionColumnNames."""
                         )
      fs              <- Hive.withConf(FileSystem.get(_))
      tablePath       <- Hive.value(fs.makeQualified(new Path(mTable.getSd.getLocation)).toString)
      (valid, invalid) = paths.map(fs.makeQualified(_).toString).partition(_.startsWith(tablePath))
      partitionPaths  <- if (invalid.isEmpty) Hive.value(valid.map(_.stripPrefix(tablePath + "/"))) else 
                           Hive.fail[List[String]](s"$invalid are not valid partition paths for $database.$table")
      partitionPattern = pattern(mTable.getPartitionKeys.asScala.map(c => c.getName).toList) // Using metadata for the pattern
      partitions      <- Hive.result(partitionPaths.map(p => partition(mTable, partitionPattern, p)).sequence)
      _               <- Hive.withClient(_.add_partitions(partitions.asJava))
    } yield ()
  }
  
  /** Repair/Register partitions with the Hive table. 
    * 
    * Please be aware that if you have a large number of partitions,
    * this '''could fail''' due to any of the following reasons :-
    *  - Out of memory on the client side.
    *  - Slow network would cause transportation issues while sending the 
    *    huge object graph to the metastore.
    *  - Out of memory on the metastore side.
    *
    * In these situations, it would be best to use [[addPartitions]].
    *
    * @param database Name of the database. 
    * @param table Name of table to create.
    */
  def repair(database: String, table: String): Hive[Unit] = 
    //For some reason when running msck, we can't use the table scoped with the db like db.table
    queries(List(s"USE $database", s"MSCK REPAIR TABLE $table")).map(_ => ())

  implicit val monad: ResultantMonad[Hive] = new ResultantMonad[Hive] {
    def rPoint[A](v: => Result[A]): Hive[A] = Hive[A]((_, _) => v)
    def rBind[A, B](ma: Hive[A])(f: Result[A] => Hive[B]): Hive[B] =
      Hive((conf, client) => f(ma.action(conf, client)).action(conf, client))
  }
}
