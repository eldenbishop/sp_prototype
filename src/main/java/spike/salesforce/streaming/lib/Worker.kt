package spike.salesforce.streaming.lib

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.LongDeserializer

val BOOTSTRAP_SERVERS_CONFIG = "ebishop-ltm1.internal.salesforce.com:9092"
//val BOOTSTRAP_SERVERS_CONFIG = "127.0.0.1:9092"

fun buildProducer(clientId: String) : KafkaProducer<Long,String> {
    val props = Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer::class.java!!.getName())
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java!!.getName())
    return KafkaProducer<Long,String>(props)
}

fun buildConsumer(groupId: String, topic: String): Consumer<Long, String> {
    val props = Properties()
    props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVERS_CONFIG
    props[ConsumerConfig.GROUP_ID_CONFIG] = groupId
    props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = LongDeserializer::class.java.name
    props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
    props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false" // *** TODO: we don't commit, explain
    props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "latest" // *** TODO: explain
    val consumer = KafkaConsumer<Long,String>(props)
    consumer.subscribe(Collections.singletonList(topic))
    return consumer
}

class Worker(
        val port: Int,
        val roles: List<String>
) {
    fun start() {

        val publisher = buildProducer("stream-processing")

        // Launch the monitor/master singleton (negotiated with a lock)
        // *** normally all workers would run this but we need isolated roles for the demo
        if (roles.contains("monitor")) {
            Monitor(buildConsumer("sp-monitor", "monitor"), publisher)
        }

        // *** Launch the supervisor
        // *** normally all workers would run this but we need isolated roles for the demo
        if (roles.contains("supervisor")) {
            Supervisor(buildConsumer("sp-supervisor", "supervisor"), publisher)
        }

        // *** Launch the REST API
        RestApi(publisher, port).start()
    }
}