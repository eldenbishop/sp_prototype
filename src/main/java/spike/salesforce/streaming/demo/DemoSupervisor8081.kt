package spike.salesforce.streaming.demo

import spike.salesforce.streaming.lib.Worker

/*
Runs a supervisor on port 8081
 */
fun main() {
    Worker(8081, listOf("supervisor")).start()
}