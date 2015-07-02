//   Copyright 2015 Commonwealth Bank of Australia
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

package au.com.cba.omnia.ebenezer.scrooge

import com.twitter.scrooge.{ThriftStructField, ThriftStructCodec, ThriftStruct}

import org.apache.thrift.protocol.TField

/** This object provides com.twitter.scrooge.ThriftStructMetaData information,
 *  using copies of the code found in that module (version 3.17). Metadata
 *  cannot be used on thrift union fields (until scrooge version 3.19), hence
 *  this ugly and brittle workaround.
 */
object ScroogeMetaHelper {
  /** verbatim copy of scrooge 3.17::com.twitter.scrooge.ThriftStructMetaData.scala */
  def toCamelCase(str: String): String = {
    str.takeWhile(_ == '_') +
      str
        .split('_')
        .filterNot(_.isEmpty)
        .zipWithIndex.map { case (part, ind) =>
          val first = if (ind == 0) part(0).toLower else part(0).toUpper
          val isAllUpperCase = part.forall(_.isUpper)
          val rest = if (isAllUpperCase) part.drop(1).toLowerCase else part.drop(1)
          new StringBuilder(part.size).append(first).append(rest)
        }
        .mkString
  }

  /** This code is mostly a copy of com.twitter.scrooge.ThriftStructMetaData.fields
    * in scrooge 3.17, but handles the /method/ retrieval failures which occur for unions.
    */
  //
  def fieldsInStruct[T <: ThriftStruct](codec: ThriftStructCodec[T]): Seq[ThriftStructField[T]] = {
    // find out names and classes
    val codecClass  = codec.getClass
    val structClass = codecClass
        .getClassLoader
        .loadClass(codecClass.getName.init) // drop '$' from object name
        .asInstanceOf[Class[T]]

    // retrieve the fields
    codecClass.getMethods.toList.filter(m =>
      m.getParameterTypes.isEmpty && m.getReturnType == classOf[TField]
    ).map { m =>
      val tfield = m.invoke(codec).asInstanceOf[TField]

      val manifest: scala.Option[Manifest[_]] = try {
        Some(
          codecClass
            .getMethod(m.getName + "Manifest")
            .invoke(codec)
            .asInstanceOf[Manifest[_]]
        )
      } catch {
        case _: Throwable => None
      }

      val method = try {
        structClass.getMethod(toCamelCase(tfield.name))
        // Method does not exist for fields that are union members, we return null.
        // Needs matching code in ScroogeStructConverter.isUnionField
      } catch { case _: NoSuchMethodException => null }

      new ThriftStructField[T](tfield, method, manifest)
    }
  }

}
