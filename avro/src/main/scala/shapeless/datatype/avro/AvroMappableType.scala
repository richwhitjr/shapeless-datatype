package shapeless.datatype.avro

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import shapeless.datatype.mappable.{BaseMappableType, MappableType}

import scala.collection.JavaConverters._

trait BaseAvroMappableType[V] extends MappableType[(GenericRecord, Schema), V] {
  def from(value: AnyRef): V
  def to(value: V): AnyRef

  override def get(m: (GenericRecord, Schema), key: String): Option[V] =
    Option(m._1.get(key)).map(from)
  override def getAll(m: (GenericRecord, Schema), key: String): Seq[V] = m._1.get(key) match {
    case null => Nil
    case v => v.asInstanceOf[java.util.List[AnyRef]].asScala.map(from)
  }

  override def put(key: String, value: V, tail: (GenericRecord, Schema)): (GenericRecord, Schema) = ???

  override def put(key: String, value: Option[V], tail: (GenericRecord, Schema)): (GenericRecord, Schema) = ???

  override def put(key: String, values: Seq[V], tail: (GenericRecord, Schema)): (GenericRecord, Schema) = ???
}

trait AvroMappableType {
  implicit val avroBaseMappableType = new BaseMappableType[GenericRecord] {
    override def base: GenericRecord = ???

    override def get(m: GenericRecord, key: String): Option[GenericRecord] = ???

    override def getAll(m: GenericRecord, key: String): Seq[GenericRecord] = ???

    override def put(key: String, value: GenericRecord, tail: GenericRecord): GenericRecord = ???

    override def put(key: String, value: Option[GenericRecord], tail: GenericRecord): GenericRecord = ???

    override def put(key: String, values: Seq[GenericRecord], tail: GenericRecord): GenericRecord = ???
  }

}
