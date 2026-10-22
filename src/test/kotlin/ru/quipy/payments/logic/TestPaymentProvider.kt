package ru.quipy.payments.logic

import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import ru.quipy.common.utils.NamedThreadFactory

internal class TestPaymentProvider(
    private val response: (Int) -> Reply = { Reply() },
) : AutoCloseable {
    data class Reply(val success: Boolean = true, val status: Int = 200, val malformed: Boolean = false)

    val received = LinkedBlockingQueue<UUID>()
    val maxActive = AtomicInteger()
    private val active = AtomicInteger()
    private val requestNumber = AtomicInteger()
    private val executor = Executors.newCachedThreadPool(NamedThreadFactory("provider-test"))
    private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
    val address: String get() = "127.0.0.1:" + server.address.port

    init {
        server.executor = executor
        server.createContext("/external/process") { exchange ->
            val params = exchange.requestURI.rawQuery.split('&').associate {
                val (key, value) = it.split('=', limit = 2)
                key to URLDecoder.decode(value, UTF_8)
            }
            val paymentId = UUID.fromString(params.getValue("paymentId"))
            val transactionId = params.getValue("transactionId")
            val concurrent = active.incrementAndGet()
            maxActive.updateAndGet { maxOf(it, concurrent) }
            val index = requestNumber.incrementAndGet()
            received.add(paymentId)
            val reply = try {
                response(index)
            } finally {
                active.decrementAndGet()
            }
            val success = reply.success
            val body = if (reply.malformed) "not json" else
                """{"transactionId":"$transactionId","paymentId":"$paymentId","result":$success,"message":null}"""
            val bytes = body.toByteArray(UTF_8)
            exchange.responseHeaders.add("Content-Type", "application/json")
            exchange.sendResponseHeaders(reply.status, bytes.size.toLong())
            exchange.responseBody.use { it.write(bytes) }
            exchange.close()
        }
        server.start()
    }

    override fun close() {
        server.stop(0)
        executor.shutdownNow()
    }
}
