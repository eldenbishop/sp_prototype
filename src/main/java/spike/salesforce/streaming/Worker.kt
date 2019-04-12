package spike.salesforce.streaming

import com.google.gson.GsonBuilder
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
import org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG

val BOOTSTRAP_SERVERS_CONFIG = "127.0.0.1:9092"

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
    props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = "false"
    props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
    val consumer = KafkaConsumer<Long,String>(props)
    consumer.subscribe(Collections.singletonList(topic))
    return consumer
}

fun hasRole(role: String, args: Array<String>) = args.asList().contains(role)

fun main(args: Array<String>) {

    val port = Integer.parseInt(args[0])

    val gson = GsonBuilder().setPrettyPrinting().create()
    val producer = buildProducer("stream-processing")

    val api = Api(producer, port)
    api.start()

    if (hasRole("monitor", args)) {
        val monitorConsumer = buildConsumer("sp-monitor", "monitor")
        val monitor = Monitor(gson, monitorConsumer, producer)
    }

    if (hasRole("supervisor", args)) {
        val supervisorConsumer = buildConsumer("sp-supervisor", "supervisor")
        val supervisor = Supervisor(gson, supervisorConsumer, producer)
    }

}
