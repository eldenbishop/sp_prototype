package spike.salesforce.streaming.demo

import spike.salesforce.streaming.lib.Worker

/*
Runs a supervisor on port 8083
 */
fun main() {
    Worker(8083, listOf("supervisor")).start()
}