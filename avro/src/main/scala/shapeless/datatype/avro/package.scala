package shapeless.datatype

import org.apache.avro.generic.GenericRecord
import shapeless._
import shapeless.datatype.mappable.{CanNest, FromMappable, ToMappable}

package object avro {
  type Record = Either[RecordBuilder, GenericRecord]
  type FromGenericRecord[L <: HList] = FromMappable[L, Record]
  type ToGenericRecord[L <: HList] = ToMappable[L, Record]

  implicit object AvroCanNest extends CanNest[Record]
}
