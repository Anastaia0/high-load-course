package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
import okio.Buffer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Answers
import org.mockito.Mockito
import org.mockito.stubbing.Answer
import ru.quipy.core.EventSourcingService
import ru.quipy.domain.Event
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.PaymentCreatedEvent
import ru.quipy.payments.api.PaymentProcessedEvent
import ru.quipy.payments.api.PaymentSubmittedEvent
import ru.quipy.payments.metrics.PaymentMetrics
import java.net.SocketTimeoutException
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.FutureTask
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

@Timeout(30)
class PaymentExternalSystemAdapterTest {
    @ParameterizedTest
    @ValueSource(longs = [0, 1000, 11000])
    fun `expired or insufficient budgets produce terminal events without a paid request`(deadline: Long) {
        val fixture = Fixture()
        fixture.pay(fixture.createPayment(), deadline)

        assertTrue(fixture.requests.isEmpty())
        assertEquals(0.0, fixture.submittedCount())
        assertEquals(1.0, fixture.completedCount("deadline_exceeded"))
        assertFalse(fixture.events.filterIsInstance<PaymentSubmittedEvent>().single().success)
        val result = fixture.events.filterIsInstance<PaymentProcessedEvent>().single()
        assertFalse(result.success)
        assertEquals("Insufficient time before payment deadline.", result.reason)
    }

    @Test
    fun `a sufficient budget preserves the request and records its outcome once`() {
        val fixture = Fixture()
        val paymentId = fixture.createPayment()
        fixture.pay(paymentId, 20_000)

        val request = fixture.requests.single()
        assertEquals("PUT", request.method)
        assertEquals("/external/process/bulk", request.url.encodedPath)
        assertEquals("acc-5", fixture.body(request).accountName)
        assertEquals(paymentId.toString(), fixture.body(request).requests.single().paymentId)
        assertEquals(1.0, fixture.submittedCount())
        assertEquals(1.0, fixture.providerRequestCount())
        assertEquals(1.0, fixture.completedCount("success"))
        assertTrue(fixture.events.filterIsInstance<PaymentSubmittedEvent>().single().success)
        assertTrue(fixture.events.filterIsInstance<PaymentProcessedEvent>().single().success)
    }

    @Test
    fun `the budget is checked after waiting for an occupied account slot`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val fixture = Fixture { _ ->
            entered.countDown()
            assertTrue(release.await(5, TimeUnit.SECONDS))
        }
        val first = FutureTask { fixture.pay(fixture.createPayment(), 20_000) }
        Thread(first, "first-payment").start()
        assertTrue(entered.await(5, TimeUnit.SECONDS))
        val waitingId = fixture.createPayment()
        val waiting = FutureTask { fixture.pay(waitingId, 12000) }
        val waitingThread = Thread(waiting, "waiting-for-payment-slot")
        waitingThread.start()
        try {
            awaitState(waitingThread, Thread.State.WAITING)
            fixture.clock.time.set(3000)
        } finally {
            release.countDown()
        }
        first.get(5, TimeUnit.SECONDS)
        waiting.get(5, TimeUnit.SECONDS)

        assertEquals(1, fixture.requests.size)
        assertEquals(1.0, fixture.completedCount("success"))
        assertEquals(1.0, fixture.completedCount("deadline_exceeded"))
        assertFalse(fixture.events.filterIsInstance<PaymentSubmittedEvent>().single { it.paymentId == waitingId }.success)
    }

    @Test
    fun `the budget is checked again after waiting for a rate permit`() {
        val fixture = Fixture(rate = 1)
        fixture.pay(fixture.createPayment(), 20_000)
        val waiting = FutureTask { fixture.pay(fixture.createPayment(), 12000) }
        val waitingThread = Thread(waiting, "waiting-for-rate-permit")
        waitingThread.start()
        awaitState(waitingThread, Thread.State.TIMED_WAITING)
        fixture.clock.time.set(3000)
        waiting.get(5, TimeUnit.SECONDS)

        assertEquals(1, fixture.requests.size)
        assertEquals(1.0, fixture.submittedCount())
        assertEquals(1.0, fixture.completedCount("deadline_exceeded"))
    }

    @Test
    fun `a transport timeout releases the batch slot without retrying any payment`() {
        val calls = AtomicInteger()
        val fixture = Fixture { _ ->
            if (calls.incrementAndGet() == 1) throw SocketTimeoutException("Simulated provider timeout")
        }
        fixture.payBatch(List(3) { fixture.payment() })
        fixture.payBatch(List(2) { fixture.payment() })

        assertEquals(2, fixture.requests.size)
        assertEquals(5.0, fixture.submittedCount())
        assertEquals(2.0, fixture.providerRequestCount())
        assertEquals(3.0, fixture.completedCount("timeout"))
        assertEquals(2.0, fixture.completedCount("success"))
        assertEquals(5, fixture.events.filterIsInstance<PaymentSubmittedEvent>().size)
        assertEquals(5, fixture.events.filterIsInstance<PaymentProcessedEvent>().size)
    }

    @Test
    fun `five payments use one provider request and retain individual identities and amounts`() {
        val fixture = Fixture(transform = { it.reversed() })
        val payments = List(5) { fixture.payment(amount = 100 + it) }
        fixture.payBatch(payments)

        val body = fixture.body(fixture.requests.single())
        assertEquals("test-service", body.serviceName)
        assertEquals("acc-5", body.accountName)
        assertEquals(payments.map { it.paymentId.toString() }, body.requests.map { it.paymentId })
        assertEquals(payments.map { it.amount }, body.requests.map { it.amount })
        assertEquals(5, body.requests.map { it.transactionId }.toSet().size)
        assertEquals(1.0, fixture.providerRequestCount())
        assertEquals(5.0, fixture.submittedCount())
        assertEquals(5.0, fixture.completedCount("success"))
        assertEquals(payments.map { it.paymentId }.toSet(), fixture.events.filterIsInstance<PaymentProcessedEvent>().map { it.paymentId }.toSet())
    }

    @Test
    fun `a partial failure is matched by identity even when responses arrive in reverse order`() {
        val fixture = Fixture(transform = { responses ->
            responses.mapIndexed { index, response ->
                ExternalSysResponse(response.transactionId, response.paymentId, index != 1, if (index == 1) "Temporary error" else null)
            }.reversed()
        })
        val payments = List(3) { fixture.payment() }
        fixture.payBatch(payments)

        val outcomes = fixture.events.filterIsInstance<PaymentProcessedEvent>().associateBy { it.paymentId }
        assertTrue(outcomes.getValue(payments[0].paymentId).success)
        assertFalse(outcomes.getValue(payments[1].paymentId).success)
        assertTrue(outcomes.getValue(payments[2].paymentId).success)
        assertEquals(2.0, fixture.completedCount("success"))
        assertEquals(1.0, fixture.completedCount("failed"))
        assertEquals(1.0, fixture.providerRequestCount())
    }

    @ParameterizedTest
    @ValueSource(strings = ["missing", "duplicate", "foreign", "wrong_transaction"])
    fun `an invalid bulk response cannot produce successful payment events`(kind: String) {
        val fixture = Fixture(transform = { responses ->
            when (kind) {
                "missing" -> responses.dropLast(1)
                "duplicate" -> listOf(responses[0], responses[0])
                "foreign" -> listOf(responses[0], ExternalSysResponse(responses[1].transactionId, UUID.randomUUID().toString(), true))
                else -> listOf(responses[0], ExternalSysResponse(UUID.randomUUID().toString(), responses[1].paymentId, true))
            }
        })
        fixture.payBatch(List(2) { fixture.payment() })

        assertEquals(1, fixture.requests.size)
        assertEquals(0.0, fixture.completedCount("success"))
        assertEquals(2.0, fixture.completedCount("error"))
        assertEquals(2, fixture.events.filterIsInstance<PaymentProcessedEvent>().size)
        assertTrue(fixture.events.filterIsInstance<PaymentProcessedEvent>().none { it.success })
    }

    @Test
    fun `one expired payment does not discard the eligible entries of the batch`() {
        val fixture = Fixture()
        val expired = fixture.payment(deadline = 1000)
        val eligible = fixture.payment()
        fixture.payBatch(listOf(expired, eligible))

        assertEquals(eligible.paymentId.toString(), fixture.body(fixture.requests.single()).requests.single().paymentId)
        assertEquals(1.0, fixture.submittedCount())
        assertEquals(1.0, fixture.completedCount("success"))
        assertEquals(1.0, fixture.completedCount("deadline_exceeded"))
    }

    private fun awaitState(thread: Thread, expected: Thread.State) {
        val end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (thread.isAlive && thread.state != expected && System.nanoTime() < end) {
            Thread.sleep(1)
        }
        assertEquals(expected, thread.state)
    }

    private class MutableClock : Clock() {
        val time = AtomicLong(1000)
        override fun millis() = time.get()
        override fun instant(): Instant = Instant.ofEpochMilli(millis())
        override fun getZone(): ZoneId = ZoneOffset.UTC
        override fun withZone(zone: ZoneId): Clock = this
    }

    private class Fixture(
        rate: Int = 1000,
        transform: (List<ExternalSysResponse>) -> List<ExternalSysResponse> = { it },
        onRequest: (Request) -> Unit = {},
    ) {
        val clock = MutableClock()
        val requests = CopyOnWriteArrayList<Request>()
        val events = CopyOnWriteArrayList<Event<PaymentAggregate>>()
        private val states = ConcurrentHashMap<UUID, PaymentAggregateState>()
        private val registry = SimpleMeterRegistry()
        private val mapper = ObjectMapper().registerKotlinModule()

        @Suppress("UNCHECKED_CAST")
        private val eventService = Mockito.mock(EventSourcingService::class.java, Answer { invocation ->
            if (invocation.method.name != "update") {
                Answers.RETURNS_DEFAULTS.answer(invocation)
            } else {
                val state = states.getValue(invocation.arguments.first() as UUID)
                val command = invocation.arguments.last() as (PaymentAggregateState) -> Event<PaymentAggregate>
                val event = command(state)
                when (event) {
                    is PaymentSubmittedEvent -> state.paymentSubmittedApply(event)
                    is PaymentProcessedEvent -> state.paymentSubmittedApply(event)
                }
                events.add(event)
                event
            }
        }) as EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

        private val client = OkHttpClient.Builder().addInterceptor { chain ->
            val request = chain.request()
            requests.add(request)
            onRequest(request)
            val responses = body(request).requests.map { ExternalSysResponse(it.transactionId, it.paymentId, true) }
            val response = mapper.writeValueAsString(ExternalSysBulkResponse(transform(responses)))
            Response.Builder().request(request).protocol(Protocol.HTTP_1_1).code(200).message("OK")
                .body(response.toResponseBody("application/json".toMediaType())).build()
        }.build()

        private val adapter = PaymentExternalSystemAdapterImpl(
            PaymentAccountProperties("test-service", "acc-5", 1, rate, 30, Duration.ofMillis(4900), true),
            eventService,
            "provider.invalid",
            "test-token",
            PaymentMetrics(registry, "test-service").forAccount("acc-5"),
            clock,
            client,
        )

        fun createPayment(amount: Int = 100): UUID {
            val id = UUID.randomUUID()
            states[id] = PaymentAggregateState().apply {
                paymentCreatedApply(PaymentCreatedEvent(id, UUID.randomUUID(), amount, clock.millis()))
            }
            return id
        }

        fun pay(id: UUID, deadline: Long) = adapter.performPaymentAsync(id, 100, clock.millis(), deadline)
        fun payment(amount: Int = 100, deadline: Long = 20_000) = PaymentToProcess(createPayment(amount), amount, clock.millis(), deadline)
        fun payBatch(payments: List<PaymentToProcess>) = adapter.performPaymentsAsync(payments)
        fun body(request: Request): ExternalSysBulkRequest {
            val buffer = Buffer()
            request.body!!.writeTo(buffer)
            return mapper.readValue(buffer.readUtf8(), ExternalSysBulkRequest::class.java)
        }
        fun submittedCount() = registry.get("shop.payment.submitted").counter().count()
        fun providerRequestCount() = registry.get("shop.payment.provider.requests").counter().count()
        fun completedCount(result: String) = registry.get("shop.payment.completed").tag("result", result).counter().count()
    }
}
