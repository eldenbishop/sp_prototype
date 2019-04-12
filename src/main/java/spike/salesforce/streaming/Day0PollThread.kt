package spike.salesforce.streaming

class Day0PollThread(val superTenantId: String) : Thread() {

    var stopping = false

    override fun run() {
        var pollCount = 0
        while (!stopping) {
            println("Poll #${pollCount++} for $superTenantId")
            Thread.sleep(1000)
        }
    }

}