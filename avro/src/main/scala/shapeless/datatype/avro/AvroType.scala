package shapeless.datatype.avro

import org.apache.avro.generic.GenericRecord
import shapeless._

import scala.reflect.runtime.universe._

class AvroType[A: TypeTag] extends Serializable {
  def fromGenericRecord[L <: HList](m: GenericRecord)
                                   (implicit gen: LabelledGeneric.Aux[A, L], fromL: FromGenericRecord[L])
  : Option[A] = fromL(Right(m)).map(gen.from)
  def toGenericRecord[L <: HList](a: A)
                                 (implicit gen: LabelledGeneric.Aux[A, L], toL: ToGenericRecord[L])
  : GenericRecord = toL(gen.to(a)).left.get.build(AvroSchema[A])
}

object AvroType {
  def apply[A: TypeTag]: AvroType[A] = new AvroType[A]
}

object Test {
  def main(args: Array[String]): Unit = {
    val at = AvroType[Record0]
    println(at.toGenericRecord(Record0("a", 1)))
  }
}

case class Record0(a: String, b: Int)

case class Record1(b: Boolean, i: Int, l: Long, f: Float, d: Double, s: String, ba: Array[Byte])
case class Record2(b: Option[Boolean],
                   i: Option[Int], l: Option[Long],
                   f: Option[Float], d: Option[Double],
                   s: Option[String],
                   ba: Option[Array[Byte]])
case class Record3(b: List[Boolean],
                   i: List[Int], l: List[Long],
                   f: List[Float], d: List[Double],
                   s: List[String],
                   ba: List[Array[Byte]])
case class Record4(r1: Record1, r2: Record2, r3: Record3,
                   o1: Option[Record1], o2: Option[Record2], o3: Option[Record3],
                   l1: List[Record1], l2: List[Record2], l3: List[Record3])