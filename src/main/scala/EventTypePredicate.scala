import model.Event
import org.apache.kafka.streams.kstream.Predicate
import org.json4s.native.Serialization
import org.json4s.native.Serialization.read
import org.json4s.{Formats, NoTypeHints}

class EventTypePredicate(eventType: String) extends Predicate[String, String] {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def test(key: String, value: String): Boolean = {
    val event = read[Event](value)
    event.eventType == eventType
  }
}
