package serialisation

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util

import org.apache.kafka.common.serialization.Deserializer

import scala.collection.mutable

class SeqDeserialiser[ELEMENT] extends Deserializer[Seq[ELEMENT]] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
  }

  override def deserialize(topic: String, data: Array[Byte]): Seq[ELEMENT] = {
    val byteStream = new ByteArrayInputStream(data)
    val objectStream = new ObjectInputStream(byteStream)
    val result = mutable.Seq[ELEMENT]()
    while (objectStream.available() > 0) {
      result :+ objectStream.readObject().asInstanceOf[ELEMENT]
    }
    objectStream.close()
    result
  }

  override def close(): Unit = {
  }
}
