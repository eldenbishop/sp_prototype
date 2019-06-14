package spike.salesforce.streaming.demo

import spike.salesforce.streaming.lib.Worker

/*
Runs a monitor on port 9090
 */
fun main() {
    Worker(9090, listOf("monitor")).start()
}