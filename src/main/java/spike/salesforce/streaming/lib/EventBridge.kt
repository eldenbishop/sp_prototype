package spike.salesforce.streaming.lib

fun getPartitionForSuperTenant(superTenantId: String, numPartitions: Int) : Int {
    return Math.abs(superTenantId.hashCode()) % numPartitions
}

class EventBridge(val partition: Int) {

    val pollThreads = HashMap<String, FakeDay0PollThread>()

    fun day0Start(superTenantId: String) {
        synchronized(pollThreads) {
            if (!pollThreads.containsKey(superTenantId)) {
                val pollThread = FakeDay0PollThread(superTenantId)
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
            val threads = ArrayList<Thread>()
            // *** tell all the threads to exit
            pollThreads.keys.toList().forEach {
                val pollThread = pollThreads.remove(it)!!
                pollThread.stopping = true
                threads.add(pollThread)
            }
            // *** now wait for each thread to exit
            threads.forEach {
                it.join()
            }
        }
    }

}