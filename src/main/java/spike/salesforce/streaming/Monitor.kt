package spike.salesforce.streaming

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import java.time.Duration
import java.util.*

class Monitor(
    val gson: Gson,
    val consumer: Consumer<Long, String>,
    val producer: Producer<Long, String>
) : Runnable, ConsumerRebalanceListener {

    val numSupervisorPartitions: Int
    val day0Subscriptions = HashSet<String>()
    val thread = Thread(this)

    init {
        numSupervisorPartitions = consumer.partitionsFor("supervisor").size
        consumer.subscribe(Collections.singletonList("monitor"), this)
        thread.isDaemon = true
        thread.start()
    }

    override fun run() {
        while (true) {
            val records = consumer.poll(Duration.ofSeconds(5))
            if (!records.isEmpty) {
                records.forEach { record ->
                    println(record)
                    val jsonString = record.value()
                    val map = gson.fromJson(jsonString, Map::class.java)
                    val action = map["type"]!! as String
                    when(action) {
                        "day0Start" -> {
                            val superTenantId = map["superTenantId"]!! as String
                            day0Subscriptions.add(superTenantId)
                            forwardToSupervisor(superTenantId, jsonString)
                        }
                        "day0Stop" -> {
                            val superTenantId = map["superTenantId"]!! as String
                            day0Subscriptions.remove(superTenantId)
                            forwardToSupervisor(superTenantId, jsonString)
                        }
                        "supervisorRefresh" -> {
                            day0Subscriptions.forEach { superTenantId ->
                                forwardToSupervisor(
                                    superTenantId,
                                    gson.toJson(linkedMapOf(
                                        "type" to "day0Start",
                                        "superTenantId" to superTenantId
                                    ))
                                )
                            }
                        }
                    }
                    println("got " + action)
                    println(map)
                    println("day0Subscriptions = ${day0Subscriptions}")
                }
                consumer.commitAsync()
            }
        }
    }

    fun forwardToSupervisor(superTenantId: String, value: String) {
        forwardToSupervisor(getPartitionForSuperTenant(superTenantId, numSupervisorPartitions), value)
    }

    fun forwardToSupervisor(partition: Int, value: String) {
        producer.send(ProducerRecord(
            "supervisor",
            partition,
            partition.toLong(),
            value
        ))
    }

    override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>?) {
        println("Monitor.onPartitionsAssignement")
        partitions?.forEach {
            println("assigned: " + it)
        }
    }

    override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>?) {
        println("Monitor.onPartitionsRevoked")
        partitions?.forEach {
            println("revoked: " + it)
        }
    }

}
