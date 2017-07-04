package serialisation

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util

import org.apache.kafka.common.serialization.Serializer

class SeqSerialiser[ELEMENT] extends Serializer[Seq[ELEMENT]] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
  }

  override def serialize(topic: String, data: Seq[ELEMENT]): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream()
    val objectStream = new ObjectOutputStream(byteStream)
    data.foreach(objectStream.writeObject(_))
    objectStream.close()
    byteStream.toByteArray
  }

  override def close(): Unit = {
  }
}
