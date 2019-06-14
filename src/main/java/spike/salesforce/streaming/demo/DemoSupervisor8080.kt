package spike.salesforce.streaming.demo

import spike.salesforce.streaming.lib.Worker

/*
Runs a supervisor on port 8080
 */
fun main() {
    val candidatePort = System.getProperty("worker.port")
    val port = if (candidatePort != null) Integer.parseInt(candidatePort) else 8080
    Worker(8080, listOf("supervisor")).start()
}