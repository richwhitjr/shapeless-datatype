package shapeless.datatype.avro

import org.apache.avro.{JsonProperties, Schema}
import org.apache.avro.Schema.Field

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._

object AvroSchema {

  private def isField(s: Symbol): Boolean =
    s.isPublic && s.isMethod && !s.isSynthetic && !s.isConstructor

  private def isCaseClass(tpe: Type): Boolean =
    !tpe.toString.startsWith("scala.") &&
      List(typeOf[Product], typeOf[Serializable], typeOf[Equals])
        .forall(b => tpe.baseClasses.contains(b.typeSymbol))

  private def toSchema(tpe: Type): (Schema, Any) = tpe match {
    case t if t =:= typeOf[Boolean] => (Schema.create(Schema.Type.BOOLEAN), null)
    case t if t =:= typeOf[Int] => (Schema.create(Schema.Type.INT), null)
    case t if t =:= typeOf[Long] => (Schema.create(Schema.Type.LONG), null)
    case t if t =:= typeOf[Float] => (Schema.create(Schema.Type.FLOAT), null)
    case t if t =:= typeOf[Double] => (Schema.create(Schema.Type.DOUBLE), null)
    case t if t =:= typeOf[String] => (Schema.create(Schema.Type.STRING), null)
    case t if t =:= typeOf[Array[Byte]] => (Schema.create(Schema.Type.BYTES), null)

    case t if t.erasure =:= typeOf[Option[_]].erasure =>
      val s = toSchema(t.typeArgs.head)._1
      (Schema.createUnion(Schema.create(Schema.Type.NULL), s), JsonProperties.NULL_VALUE)
    case t if t.erasure <:< typeOf[Traversable[_]].erasure =>
      val s = toSchema(t.typeArgs.head)._1
      (Schema.createArray(s), java.util.Collections.emptyList())

    case t if isCaseClass(t) =>
      val fields: List[Field] = t.decls.filter(isField).map(toField)(scala.collection.breakOut)
      val name = t.typeSymbol.name.toString
      val pkg = t.typeSymbol.owner.fullName
      (Schema.createRecord(name, null, pkg, false, fields.asJava), null)
  }

  private def toField(s: Symbol): Field = {
    val name = s.name.toString
    val tpe = s.asMethod.returnType
    val (schema, default) = toSchema(tpe)
    new Field(name, schema, null, default)
  }

  def apply[T: TypeTag]: Schema = {
    val tt = implicitly[TypeTag[T]]
    toSchema(tt.tpe)._1
  }

}
