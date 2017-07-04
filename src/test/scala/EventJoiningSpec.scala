import model.Event
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreamsAllInOne
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.Stores
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}
import org.json4s.{Formats, NoTypeHints}
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.{Logger, LoggerFactory}
import serialisation.{EventDeserialiser, EventSerialiser, SeqDeserialiser, SeqSerialiser}
import transformation.{EventCollector, StringEventCollector}

class EventJoiningSpec
  extends FlatSpec with EmbeddedKafkaStreamsAllInOne with Matchers {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  val log: Logger = LoggerFactory.getLogger(this.getClass)

  implicit val stringSerialiser: StringSerializer = new StringSerializer
  implicit val stringDeserialiser: StringDeserializer = new StringDeserializer

  implicit val eventSerialiser: EventSerialiser = new EventSerialiser
  implicit val eventDeserialiser: EventDeserialiser = new EventDeserialiser

  val stringSerde = new StringSerde
  val seqSerde: Serde[Seq[Nothing]] = Serdes.serdeFrom(new SeqSerialiser, new SeqDeserialiser)
  val eventSerde: Serde[Event] = Serdes.serdeFrom(eventSerialiser, new EventDeserialiser)

  val fragmentTopic = "event_fragments"
  val mergedTopic = "merged_events"
  val collectTopic = "collected_events"
  val keyBlue = "blue"
  val keyGrey = "red"
  val storeName = "fragment_store"

  val brokerConfig: Map[String, String] = Map(
    "flush.messages" -> "1",
    "flush.ms" -> "100")

  val producerConfig: Map[String, String] = Map(
    ProducerConfig.BATCH_SIZE_CONFIG -> "0",
    ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG -> "10000")

  val consumerConfig: Map[String, String] = Map(
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "500")

  val kafkaConfig: EmbeddedKafkaConfig = new EmbeddedKafkaConfig(
    customBrokerProperties = brokerConfig,
    customProducerProperties = producerConfig,
    customConsumerProperties = consumerConfig)

  val forEacher = new ForeachAction[String, String] {
    def apply(key: String, value: String): Unit = {
      log.info(s"model.Event with key : $key and value : $value")
    }
  }

  //  "Two events with the same key" should
//    "be joined into a single output event using a join window" in {
//      val predicateTypeA = new EventTypePredicate(typeA)
//      val predicateTypeB = new EventTypePredicate(typeB)
//
//      Stores.create(storeName)
//        .withStringKeys()
//        .withStringValues()
//        .inMemory()
//        .build()
//
//      val streamBuilder = new KStreamBuilder
//      val inputStream = streamBuilder.stream[String, String](stringSerde, stringSerde, fragmentTopic)
//      val typeAs = inputStream.filter(predicateTypeA)
//      val typeBs = inputStream.filter(predicateTypeB)
//
//      typeAs.join(typeBs)
//
//      val fragmentGroupStream = inputStream.groupByKey(stringSerde, stringSerde)
//
//      // TODO This seems to take quite a long time.
//      val reducedFragmentTable = fragmentGroupStream.reduce(reducer, storeName)
//
//      reducedFragmentTable.foreach(forEacher)
//      reducedFragmentTable.to(stringSerde, stringSerde, mergedTopic)
//
//      runStreamsWithStringConsumer(
//        Seq(fragmentTopic, mergedTopic), streamBuilder
//      ){ consumer =>
//
//        publishToKafka(fragmentTopic, eventKey, write(model.Event(typeA, eventCount, "")))
//        publishToKafka(fragmentTopic, eventKey, write(model.Event(typeB, 0, eventName)))
//
//        val mergedEvent = readFirst(consumer, 60, mergedTopic)
//        mergedEvent should not be empty
//
//        val event = read[model.Event](mergedEvent.get)
//        event.count shouldBe eventCount
//        event.name shouldBe eventName
//      }
//  }

  "Two string events with the same key" should
    "be joined into a single output event using a join window" in {
      val collectTransformerSupplier = new TransformerSupplier[String, String, KeyValue[String, String]] {
        override def get(): Transformer[String, String, KeyValue[String, String]] = {
          new StringEventCollector(storeName)
        }
      }

      val streamBuilder = new KStreamBuilder
      streamBuilder.addStateStore(Stores.create(storeName)
        .withStringKeys()
        .withStringValues()
        .inMemory().build())

      val inputStream = streamBuilder.stream[String, String](stringSerde, stringSerde, fragmentTopic)
      val mergedStream = inputStream.transform[String, String](collectTransformerSupplier, storeName)

      mergedStream.foreach(forEacher)
      mergedStream.to(stringSerde, stringSerde, mergedTopic)

      runStreamsWithStringConsumer(
        Seq(fragmentTopic, mergedTopic), streamBuilder
      ){ consumer =>

        publishToKafka(fragmentTopic, keyBlue, write(Event("A", 1, "")))
        publishToKafka(fragmentTopic, keyBlue, write(Event("B", 0, "foo")))

        val mergedEvent = readFirst(consumer, 60, mergedTopic)
        mergedEvent should not be empty

        val event = read[Event](mergedEvent.get)
        event.eventType shouldBe "AB"
        event.count shouldBe 1
        event.name shouldBe "foo"
      }
  }

  "Three Events with the same key" should
    "be joined into a single output Event" in {
      val collectTransformerSupplier = new TransformerSupplier[String, Event, KeyValue[String, Event]] {
        override def get(): Transformer[String, Event, KeyValue[String, Event]] = {
          new EventCollector(storeName)
        }
      }

      val streamBuilder = new KStreamBuilder
      streamBuilder.addStateStore(Stores.create(storeName)
        .withStringKeys()
        .withValues(seqSerde)
        .inMemory().build())

      val inputStream = streamBuilder.stream[String, Event](stringSerde, eventSerde, fragmentTopic)
      val mergedStream = inputStream.transform[String, Event](collectTransformerSupplier, storeName)

      mergedStream.to(stringSerde, eventSerde, mergedTopic)

      runStreams(Seq(fragmentTopic, mergedTopic), streamBuilder) {
        withConsumer[String, Event, Any] { consumer =>

          publishToKafka[String, Event](fragmentTopic, keyBlue, Event("blueA", 1, ""))
          publishToKafka[String, Event](fragmentTopic, keyGrey, Event("greyA", 5, "blah"))
          publishToKafka[String, Event](fragmentTopic, keyBlue, Event("blueB", 0, "foo"))
          publishToKafka[String, Event](fragmentTopic, keyBlue, Event("blueC", 2, "bar"))

          val mergedEvent = readFirst[Event](consumer, 60, mergedTopic)
          mergedEvent should not be empty

          mergedEvent.get.eventType shouldBe "blueAblueBblueC"
          mergedEvent.get.count shouldBe 2
          mergedEvent.get.name shouldBe "foobar"

          val timedOutEvent = readFirst[Event](consumer, 60, mergedTopic)
          timedOutEvent should not be empty

          timedOutEvent.get.eventType shouldBe "greyA"
          timedOutEvent.get.count shouldBe 5
          timedOutEvent.get.name shouldBe "blah"
        }
      }
  }

//  "Multiple events with the same key" should
//    "be joined into a single output event" in {
//      val reducer = new Reducer[String] {
//        def apply(firstFragment: String, secondFragment: String): String = {
//          log.info(s"Reducing ...")
//          log.info(s"   with first : $firstFragment")
//          log.info(s"   with second : $secondFragment")
//          val firstEvent = read[model.Event](firstFragment)
//          val secondEvent = read[model.Event](secondFragment)
//          write(model.Event(firstEvent.eventType + secondEvent.eventType,
//                      firstEvent.count + secondEvent.count,
//                      firstEvent.name + secondEvent.name))
//        }
//      }
//
//      Stores.create(storeName)
//        .withStringKeys()
//        .withStringValues()
//        .inMemory()
//        .build()
//
//      val streamBuilder = new KStreamBuilder
//      val inputStream = streamBuilder.stream[String, String](stringSerde, stringSerde, fragmentTopic)
//      val fragmentGroupStream = inputStream.groupByKey(stringSerde, stringSerde)
//
//      // TODO This seems to take quite a long time.
//      val reducedFragmentTable = fragmentGroupStream.reduce(reducer, storeName)
//
//      reducedFragmentTable.foreach(forEacher)
//      reducedFragmentTable.to(stringSerde, stringSerde, mergedTopic)
//
//      runStreamsWithStringConsumer(
//        Seq(fragmentTopic, mergedTopic), streamBuilder
//      ){ consumer =>
//
//        publishToKafka(fragmentTopic, eventKey, write(model.Event(typeA, eventCount, "")))
//        publishToKafka(fragmentTopic, eventKey, write(model.Event(typeB, 0, eventName)))
//
//        val mergedEvent = readFirst(consumer, 60, mergedTopic)
//        mergedEvent should not be empty
//
//        val event = read[model.Event](mergedEvent.get)
//        event.count shouldBe eventCount
//        event.name shouldBe eventName
//      }
//  }

  def readFirst[VALUE](consumer: KafkaConsumer[String, VALUE], timeoutSeconds: Int, topic: String): Option[VALUE] = {
    import scala.collection.JavaConverters._
    consumer.subscribe(List(topic).asJava)
    for (i <- 1 to timeoutSeconds) {
      log.info(s"Polling attempt $i...")
      val records = consumer.poll(1000)
      if (!records.isEmpty) {
        val event = records.iterator().next().value()
        log.info(s"Found event : $event")
        return Some(event)
      }
    }

    None
  }
}

