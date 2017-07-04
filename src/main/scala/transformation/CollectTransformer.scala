package transformation

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.slf4j.{Logger, LoggerFactory}

abstract class CollectTransformer[INPUT_K, INPUT_V, STORE_V, OUTPUT_K, OUTPUT_V](storeName: String)
  extends Transformer[INPUT_K, INPUT_V, KeyValue[OUTPUT_K, OUTPUT_V]] {

  val log: Logger = LoggerFactory.getLogger(this.getClass)

  var ctx: ProcessorContext = _
  var store: KeyValueStore[INPUT_K, STORE_V] = _

  override def init(context: ProcessorContext): Unit = {
    log.debug(s"Init ...")

    ctx = context
    store = ctx.getStateStore(storeName).asInstanceOf[KeyValueStore[INPUT_K, STORE_V]]

    ctx.schedule(100)
    /*
      TODO Currently the ProcessorContext#schedule method takes an event time parameter

      The problem with this is that if no events are received in your partition, then
      Processor#punctuate is not called.

      See https://cwiki.apache.org/confluence/display/KAFKA/KIP-138%3A+Change+punctuate+semantics

      Punctuate has been changed to fix this issue, and should be available from Kafka 0.11.0.1.

      See https://github.com/apache/kafka/pull/3055
     */
  }

  override def punctuate(timestamp: Long): KeyValue[OUTPUT_K, OUTPUT_V] = {
    log.debug(s"Punctuating ...")
    null
  }

  override def transform(key: INPUT_K, value: INPUT_V): KeyValue[OUTPUT_K, OUTPUT_V] = {
    log.debug(s"Transforming event : $value")

    val currentStoreValue = store.get(key)
    if (currentStoreValue != null && collectComplete(currentStoreValue, value)) {
      collectOutput(key, currentStoreValue, value)
    } else {
      store.put(key, appendToStore(currentStoreValue, value))
      null
    }
  }

  def appendToStore(storeValue: STORE_V, appendValue: INPUT_V): STORE_V

  def collectComplete(storeValue: STORE_V, appendValue: INPUT_V): Boolean

  def collectOutput(inputKey: INPUT_K, storeValue: STORE_V, mergeValue: INPUT_V): KeyValue[OUTPUT_K, OUTPUT_V]

  override def close(): Unit = {
    log.debug(s"Close ...")
  }
}
