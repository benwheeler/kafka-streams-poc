package serialisation

import java.util

import model.Event
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write
import org.json4s.{Formats, NoTypeHints}

class EventSerialiser extends Serializer[Event] {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  val stringSerialiser = new StringSerializer

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    stringSerialiser.configure(configs, isKey)
  }

  override def serialize(topic: String, data: Event): Array[Byte] = {
    val stringValue = write(data)
    stringSerialiser.serialize(topic, stringValue)
  }

  override def close(): Unit = {
    stringSerialiser.close()
  }
}
