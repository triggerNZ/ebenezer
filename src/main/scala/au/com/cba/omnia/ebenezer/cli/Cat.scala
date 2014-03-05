package au.com.cba.omnia.ebenezer
package cli

import au.com.cba.omnia.ebenezer.introspect._
import au.com.cba.omnia.ebenezer.fs.Glob

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration

object Cat {
  def run(patterns: List[String]): Unit = {
    val conf = new Configuration
    val paths = Glob.patterns(conf, patterns)
    val records = paths.foldLeft(Iterator[Record]())((iter, path) =>
      iter ++ ParquetIntrospectTools.iteratorFromPath(conf, path))
    records.foreach(println)
  }

  def main(args: Array[String]): Unit =
    run(args.toList)
}
