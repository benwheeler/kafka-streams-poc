package transformation

import model.Event
import org.apache.kafka.streams.KeyValue
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}
import org.json4s.{Formats, NoTypeHints}

class StringEventCollector(storeName: String)
  extends CollectTransformer[String, String, String, String, String](storeName) {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  override def appendToStore(storeValue: String, appendValue: String): String = {
    appendValue
  }

  override def collectComplete(storeValue: String, appendValue: String): Boolean = {
    // just merges two events
    storeValue.nonEmpty && appendValue.nonEmpty
  }

  override def collectOutput(inputKey: String, storeValue: String, mergeValue: String): KeyValue[String, String] = {
    new KeyValue(inputKey, collect(storeValue, mergeValue))
  }

  def collect(storeValue: String, mergeValue: String): String = {
    log.info(s"Collecting ...")
    log.info(s"   with store : $storeValue")
    log.info(s"   with event : $mergeValue")
    val firstEvent = read[Event](storeValue)
    val secondEvent = read[Event](mergeValue)
    write(Event(firstEvent.eventType + secondEvent.eventType,
      firstEvent.count + secondEvent.count,
      firstEvent.name + secondEvent.name))
  }
}
