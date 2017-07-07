package shapeless.datatype.avro

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.{GenericData, GenericRecord}
import shapeless.datatype.mappable.{BaseMappableType, MappableType}

import scala.collection.mutable.{Map => MMap}
import scala.collection.JavaConverters._

trait BaseAvroMappableType[V] extends MappableType[Record, V] {
  def from(value: Any): V
  def to(value: V): Any

  override def get(m: Record, key: String): Option[V] =
    Option(m.right.get.get(key)).map(from)
  override def getAll(m: Record, key: String): Seq[V] = m.right.get.get(key) match {
    case null => Nil
    case v => v.asInstanceOf[java.util.List[Any]].asScala.map(from)
  }

  override def put(key: String, value: V, tail: Record): Record = {
    tail.left.get.put(key, to(value))
    tail
  }
  override def put(key: String, value: Option[V], tail: Record): Record = {
    value.foreach(v => tail.left.get.put(key, to(v)))
    tail
  }
  override def put(key: String, values: Seq[V], tail: Record): Record = {
    tail.left.get.put(key, values.map(to))
    tail
  }
}

trait AvroMappableType {
  implicit val avroBaseMappableType = new BaseMappableType[Record] {
    override def base: Record = Left(RecordBuilder())

    override def get(m: Record, key: String): Option[Record] =
      Option(m.right.get.get(key)).map(v => Right(v.asInstanceOf[GenericRecord]))
    override def getAll(m: Record, key: String): Seq[Record] =
      Option(m.right.get.get(key)).toSeq
        .flatMap(_.asInstanceOf[java.util.List[GenericRecord]].asScala.map(Right(_)))

    override def put(key: String, value: Record, tail: Record): Record = {
      tail.left.get.put(key, value.left.get)
      tail
    }
    override def put(key: String, value: Option[Record], tail: Record): Record = {
      value.foreach(v => tail.left.get.put(key, v.left.get))
      tail
    }
    override def put(key: String, values: Seq[Record], tail: Record): Record = {
      tail.left.get.put(key, values.map(_.left.get))
      tail
    }
  }

  private def at[T](fromFn: Any => T, toFn: T => Any) = new BaseAvroMappableType[T] {
    override def from(value: Any): T = fromFn(value)
    override def to(value: T): Any = toFn(value)
  }

  private def id[T](x: T): Any = x.asInstanceOf[Any]
  implicit val booleanAvroMappableType = at[Boolean](_.asInstanceOf[Boolean], id)
  implicit val intAvroMappableType = at[Int](_.asInstanceOf[Int], id)
  implicit val longAvroMappableType = at[Long](_.asInstanceOf[Long], id)
  implicit val floatAvroMappableType = at[Float](_.asInstanceOf[Float], id)
  implicit val doubleAvroMappableType = at[Double](_.asInstanceOf[Double], id)
  implicit val stringAvroMappableType = at[String](_.asInstanceOf[String], id)

}

case class RecordBuilder(m: MMap[String, Any] = MMap.empty) {
  def put(key: String, value: Any): Unit = m += (key -> value)
  def build(schema: Schema): GenericRecord = {
    val r = new GenericData.Record(schema)
    schema.getFields.asScala.foreach { f =>
      val s = f.schema()
      val v = if (s.getType == Type.RECORD) {
        m(f.name()).asInstanceOf[RecordBuilder].build(s)
      } else if (s.getType == Type.UNION && s.getTypes.get(1).getType == Type.RECORD) {
        m(f.name()).asInstanceOf[RecordBuilder].build(s.getTypes.get(1))
      } else if (s.getType == Type.ARRAY && s.getElementType.getType == Type.RECORD) {
        m(f.name()).asInstanceOf[Seq[RecordBuilder]].map(_.build(s.getElementType)).asJava
      }
      r.put(f.name(), v)
    }
    r
  }
}
