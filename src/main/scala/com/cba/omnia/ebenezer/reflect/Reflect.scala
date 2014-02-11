package com.cba.omnia.ebenezer
package reflect

import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type

object Reflect {
  def companionOf(cls: Class[_]): AnyRef =
    companionOfName(cls.getName)

  def companionOfName(name: String): AnyRef =
    objectOfName(name + "$")

  def objectOfName(name: String): AnyRef =
    objectOf(Class.forName(name))

  def objectOf(cls: Class[_]): AnyRef =
    cls.getField("MODULE$").get(null)

  def invoke[A: Manifest](target: AnyRef, method: String, args: Array[AnyRef] = null): A = {
    val cls = target.getClass
    val m = cls.getMethod(method)
    var targetType = implicitly[Manifest[A]].runtimeClass.asInstanceOf[Class[A]]
    targetType.cast(m.invoke(target, target))
  }

  def parameterizedTypeOf1(generic: Type): Type = {
    val args = generic.asInstanceOf[ParameterizedType].getActualTypeArguments()
    if (args.length != 1)
      sys.error("Invalid reflection assumption, can only determine parameterized type of kind * -> *, i.e. Option[A], List[A]")
    args(0)
  }
}
