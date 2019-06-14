package spike.salesforce.streaming.lib

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import j2html.TagCreator.*
import spark.ResponseTransformer
import spark.kotlin.*
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import spark.Spark
import java.awt.Desktop
import java.net.URI


class RestApi(val producer: Producer<Long,String>, val port: Int, val host:String = "localhost") {

    var gson = JsonTransformer()

    fun start() {
        port(port)
        val url = "http://localhost:${port}"
        get("/", "*") {
            html(
                    head(
                            style("""
                                p.terminal {
                                    color:white;
                                    font-family: Courier New;
                                    border:3px solid;
                                    border-color:#000000;
                                    border-radius: 5px;
                                    background: #404050;
                                    padding: 10px;
                                }
                                b {
                                    color: #8888ff;
                                    background: #eeeeff;
                                }
                            """.trimIndent()).withType("text/css")
                    ),
                    body(
                            h1("Instructions"),
                            h2("Start Polling"),
                            p(span("> curl -X POST $url/day0/"),b("<super-tenant-id>")).withClass("terminal"),
                            h2("Stop Polling"),
                            p(span("> curl -X DELETE http://localhost:${port}/day0/"),b("<super-tenant-id>")).withClass("terminal")
                    )
            )
        }
        post("/day0/:super_tenant_id", "*") {
            val superTenantId = params(":super_tenant_id")
            sendToMonitor(linkedMapOf(
                "type" to "day0Start",
                "superTenantId" to superTenantId,
                "ts" to System.currentTimeMillis()
            ))
            gson.render(StartResponse(1, "started", superTenantId))
        }
        delete("/day0/:super_tenant_id", "*") {
            val superTenantId = params(":super_tenant_id")
            sendToMonitor(linkedMapOf(
                "type" to "day0Stop",
                "superTenantId" to superTenantId,
                "ts" to System.currentTimeMillis()
            ))
            gson.render(StopResponse(1, "stopped", superTenantId))
        }
        Spark.awaitInitialization()
        println("Local REST API started. Open $url for help.")
//        if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
//            Desktop.getDesktop().browse(URI("http://localhost:${port}"));
//        }
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