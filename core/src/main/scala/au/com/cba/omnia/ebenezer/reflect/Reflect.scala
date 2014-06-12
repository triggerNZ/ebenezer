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
package reflect

import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type

import scala.util.control.NonFatal

object Reflect {
  def companionOf(cls: Class[_]): AnyRef =
    companionOfName(cls.getName)

  def companionOfName(name: String): AnyRef =
    objectOfName(name + "$")

  def objectOfName(name: String): AnyRef =
    objectOf(Class.forName(name))

  def objectOf(cls: Class[_]): AnyRef =
    try cls.getField("MODULE$").get(null)
    catch { case NonFatal(e) => throw new RuntimeException(s"Could not find companion object for ${cls}", e) }

  def invoke[A: Manifest](target: AnyRef, method: String, args: Array[AnyRef] = Array()): A = try {
    val cls = target.getClass
    val m = cls.getMethod(method)
    m.invoke(target, args:_*).asInstanceOf[A]
  } catch { case NonFatal(e) => throw new RuntimeException(s"""Could not find invoke method <${method}> on <${target.getClass}> with args <${Option(args).map(_.toList.mkString(", ")).getOrElse("null")}>, and with expected types of <${target.getClass.getMethod(method).getParameterTypes.toList}>""", e)  }

  def parameterizedTypeOf1(generic: Type): Type = {
    val args = generic.asInstanceOf[ParameterizedType].getActualTypeArguments()
    if (args.length != 1)
      sys.error("Invalid reflection assumption, can only determine parameterized type of kind * -> *, i.e. Option[A], List[A]")
    args(0)
  }
}
