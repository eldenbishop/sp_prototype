package spike.salesforce.streaming

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import java.time.Duration

class Supervisor(
    val gson: Gson,
    val consumer: Consumer<Long, String>,
    val producer: Producer<Long, String>
) : Runnable, ConsumerRebalanceListener {

    val numPartitions: Int
    val eventBridges = HashMap<Int,EventBridge>()
    val thread: Thread

    init {
        numPartitions = consumer.partitionsFor("supervisor").size
        consumer.subscribe(listOf("supervisor"), this)
        thread = Thread(this)
        thread.isDaemon = true
        thread.start()
    }

    override fun run() {
        while (true) {
            val records = consumer.poll(Duration.ofSeconds(5))
            if (!records.isEmpty) {
                records.forEach { record ->
                    val jsonText = record.value()
                    val map = gson.fromJson(jsonText, Map::class.java)
                    val command = map["type"]!! as String
                    when (command) {
                        "day0Start" -> {
                            val superTenantId = map["superTenantId"]!! as String
                            day0Start(superTenantId)
                        }
                        "day0Stop" -> {
                            val superTenantId = map["superTenantId"]!! as String
                            day0Stop(superTenantId)
                        }
                    }
                }
                consumer.commitAsync()
            }
        }
    }

    fun day0Start(superTenantId: String) {
        eventBridges
            .get(getPartitionForSuperTenant(superTenantId, numPartitions))!!
            .day0Start(superTenantId)
    }

    fun day0Stop(superTenantId: String) {
        eventBridges
            .get(getPartitionForSuperTenant(superTenantId, numPartitions))!!
            .day0Stop(superTenantId)
    }

    override fun onPartitionsAssigned(topicPartitions: MutableCollection<TopicPartition>?) {
        synchronized(eventBridges) {
            topicPartitions?.forEach { topicPartition ->
                val removed = eventBridges.remove(topicPartition.partition())
                if (removed != null) {
                    removed.stopAll()
                }
                val toAdd = EventBridge(topicPartition.partition())
                eventBridges.put(topicPartition.partition(), toAdd)
                producer.send(ProducerRecord("monitor", 0, 0L, gson.toJson(linkedMapOf(
                    "type" to "supervisorRefresh",
                    "supervisorId" to topicPartition.partition()
                ))))
            }
        }
    }

    override fun onPartitionsRevoked(topicPartitions: MutableCollection<TopicPartition>?) {
        synchronized(eventBridges) {
            topicPartitions?.forEach { topicPartition ->
                val removed = eventBridges.remove(topicPartition.partition())
                if (removed != null) {
                    removed.stopAll()
                }
            }
        }
    }

}
