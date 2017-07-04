package serialisation

import java.util

import model.Event
import org.apache.kafka.common.serialization.{Deserializer, StringDeserializer}
import org.json4s.native.Serialization
import org.json4s.native.Serialization.read
import org.json4s.{Formats, NoTypeHints}

class EventDeserialiser extends Deserializer[Event] {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  val stringDeserialiser = new StringDeserializer

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    stringDeserialiser.configure(configs, isKey)
  }

  override def deserialize(topic: String, data: Array[Byte]): Event = {
    val stringValue = stringDeserialiser.deserialize(topic, data)
    read[Event](stringValue)
  }

  override def close(): Unit = {
    stringDeserialiser.close()
  }
}
