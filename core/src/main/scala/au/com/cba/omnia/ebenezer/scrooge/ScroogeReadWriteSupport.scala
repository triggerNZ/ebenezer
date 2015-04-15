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

package au.com.cba.omnia.ebenezer
package scrooge

import org.apache.hadoop.conf.Configuration

import com.twitter.scrooge.ThriftStruct

import parquet.hadoop.BadConfigurationException

object ScroogeReadWriteSupport {
  def setThriftClass[A <: ThriftStruct : Manifest](conf: Configuration, key: String) =
    conf.set(key: String, implicitly[Manifest[A]].runtimeClass.getName)

  def getThriftClass[A <: ThriftStruct](conf: Configuration, key: String): Class[A] = {
    val name = Option(conf.get(key)).getOrElse(fail("the thrift class conf is missing in job conf at " + key))
    try
      Class.forName(name).asInstanceOf[Class[A]]
    catch {
      case e: ClassNotFoundException =>
        fail("the class " + name + " in job conf at " + key + " could not be found", Some(e))
    }
  }

  def fail(message: String, o: Option[Throwable] = None) =
    o match {
      case None    => throw new BadConfigurationException(message)
      case Some(e) => throw new BadConfigurationException(message, e)
    }
}
