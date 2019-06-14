package spike.salesforce.streaming.demo

import spike.salesforce.streaming.lib.Worker

/*
Runs a supervisor on port 8082
 */
fun main() {
    Worker(8082, listOf("supervisor")).start()
}