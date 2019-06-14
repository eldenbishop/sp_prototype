package spike.salesforce.streaming.lib

import java.util.*

class FakeDay0PollThread(val superTenantId: String) : Thread() {

    val POLL_INTERVAL = 4000L;

    var stopping = false

    override fun run() {
        while (!stopping) {
            println("Processing $superTenantId @ ${Date()}")
            Thread.sleep(POLL_INTERVAL)
        }
    }

}