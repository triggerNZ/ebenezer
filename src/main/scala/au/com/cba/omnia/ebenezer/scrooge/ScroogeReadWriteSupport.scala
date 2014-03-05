package au.com.cba.omnia.ebenezer
package scrooge

import org.apache.hadoop.conf.Configuration

import com.twitter.scrooge.ThriftStruct

import parquet.hadoop.BadConfigurationException

object ScroogeReadWriteSupport {
  val ParquetThriftClass = "parquet.scrooge.class";

  def setThriftClass[A <: ThriftStruct](conf: Configuration, cls: Class[A]) =
    conf.set(ParquetThriftClass, cls.getName);

  def getThriftClass[A <: ThriftStruct](conf: Configuration): Class[A] = {
    val name = Option(conf.get(ParquetThriftClass)).getOrElse(fail("the thrift class conf is missing in job conf at " + ParquetThriftClass))
    try
      Class.forName(name).asInstanceOf[Class[A]]
    catch {
      case e: ClassNotFoundException =>
        fail("the class " + name + " in job conf at " + ParquetThriftClass + " could not be found", Some(e))
    }
  }

  def fail(message: String, o: Option[Throwable] = None) =
    o match {
      case None    => throw new BadConfigurationException(message)
      case Some(e) => throw new BadConfigurationException(message, e)
    }
}
