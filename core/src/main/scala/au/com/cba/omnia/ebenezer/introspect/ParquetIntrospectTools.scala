package au.com.cba.omnia.ebenezer
package introspect

import com.twitter.scrooge.ThriftStruct

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import parquet.hadoop.ParquetReader

object ParquetIntrospectTools {
  def iteratorFromPath(conf: Configuration, path: Path): Iterator[Record] = {
    val reader = new ParquetReader[Record](conf, path, new IntrospectionReadSupport)
    new Iterator[Record] {
      var state: Record = reader.read
      def hasNext = state != null
      def next = { val record = state; state = reader.read; record }
    }
  }

  def streamFromPath(conf: Configuration, path: Path): Stream[Record] =
    iteratorFromPath(conf, path).toStream

  def listFromPath(conf: Configuration, path: Path): List[Record] =
    iteratorFromPath(conf, path).toList
}
