package spike.salesforce.streaming

import org.apache.kafka.common.TopicPartition

fun getPartitionForSuperTenant(superTenantId: String, numPartitions: Int) : Int {
    return Math.abs(superTenantId.hashCode()) % numPartitions
}

class EventBridge(val partition: Int) {

    val pollThreads = HashMap<String,Day0PollThread>()

    fun day0Start(superTenantId: String) {
        synchronized(pollThreads) {
            if (!pollThreads.containsKey(superTenantId)) {
                val pollThread = Day0PollThread(superTenantId)
                pollThread.start()
                pollThreads.put(superTenantId, pollThread)
            }
        }
    }

    fun day0Stop(superTenantId: String) {
        synchronized(pollThreads) {
            val day0PollThread = pollThreads.remove(superTenantId)
            if (day0PollThread != null) {
                day0PollThread.stopping = true
                day0PollThread.join()
            }
        }
    }

    fun stopAll() {
        synchronized(pollThreads) {
            pollThreads.keys.toList().forEach {
                day0Stop(it)
            }
        }
    }

}