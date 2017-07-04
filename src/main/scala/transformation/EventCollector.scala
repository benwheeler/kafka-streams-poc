package transformation

import model.Event
import org.apache.kafka.streams.KeyValue

class EventCollector(storeName: String)
  extends CollectTransformer[String, Event, Seq[Event], String, Event](storeName) {

  def collect(storeValue: Seq[Event], mergeValue: Event): Event = {
    log.info(s"Collecting ...")
    log.info(s"   with store : $storeValue")
    log.info(s"   with event : $mergeValue")
    val events = storeValue :+ mergeValue
    var eventType = ""
    var count: Int = 0
    var name = ""
    events.foreach(event => {
      if (event.eventType.nonEmpty) {
        eventType = eventType + event.eventType
      }
      if (event.count > count) {
        count = event.count
      }
      if (event.name.nonEmpty) {
        name = name + event.name
      }
    })

    Event(eventType, count, name)
  }

  override def appendToStore(storeValue: Seq[Event], appendValue: Event): Seq[Event] = {
    if (storeValue == null) {
      log.info(s"Creating store with event : $appendValue")
      Seq(appendValue)
    } else {
      log.info(s"Adding event to store : $appendValue")
      storeValue :+ appendValue
    }
  }

  override def collectComplete(storeValue: Seq[Event], appendValue: Event): Boolean = {
    // expect 2 elements in the store, plus the appendValue
    storeValue != null && storeValue.length > 1
  }

  override def collectOutput(inputKey: String, storeValue: Seq[Event], mergeValue: Event): KeyValue[String, Event] = {
    new KeyValue(inputKey, collect(storeValue, mergeValue))
  }
}
