package spike.salesforce.streaming

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import spark.ResponseTransformer
import spark.kotlin.*
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord


class Api(val producer: Producer<Long,String>, val port: Int, val host:String = "localhost") {

    var gson = JsonTransformer(GsonBuilder().setPrettyPrinting().create())

    fun start() {
        port(port)
        get("/", "*") {
            gson.render(linkedMapOf(
                "examples" to linkedMapOf(
                    "start" to "curl -X POST http://localhost:${port}/day0/acme",
                    "stop" to "curl -X DELETE http://localhost:${port}/day0/acme"
                )
            ))
        }
        post("/day0/:super_tenant_id", "*") {
            val superTenantId = params(":super_tenant_id")
            sendToMonitor(linkedMapOf(
                "type" to "day0Start",
                "superTenantId" to superTenantId,
                "ts" to System.currentTimeMillis()
            ))
            gson.render(StartResponse(1,"started", superTenantId))
        }
        delete("/day0/:super_tenant_id", "*") {
            val superTenantId = params(":super_tenant_id")
            sendToMonitor(linkedMapOf(
                "type" to "day0Stop",
                "superTenantId" to superTenantId,
                "ts" to System.currentTimeMillis()
            ))
            gson.render(StopResponse(1,"stopped", superTenantId))
        }
    }

    fun sendToMonitor(value: Map<*,*>) {
        producer.send(ProducerRecord("monitor", 0, 0L, gson.render(value)))
    }

}

data class StartResponse(val ok: Int, val status: String, val superTenantId: String)
data class StopResponse(val ok: Int, val status: String, val superTenantId: String)

class JsonTransformer(val gson: Gson = GsonBuilder().setPrettyPrinting().create()) : ResponseTransformer {
    override fun render(model: Any?): String {
        return gson.toJson(model) + "\n"
    }
}