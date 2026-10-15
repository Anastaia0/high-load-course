package ru.quipy.payments.logic

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
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
        assertEquals("POST", request.method)
        assertEquals("acc-5", request.url.queryParameter("accountName"))
        assertEquals(paymentId.toString(), request.url.queryParameter("paymentId"))
        assertEquals(1.0, fixture.submittedCount())
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
    fun `a transport timeout releases the slot and does not duplicate the next payment`() {
        val calls = AtomicInteger()
        val fixture = Fixture { _ ->
            if (calls.incrementAndGet() == 1) throw SocketTimeoutException("Simulated provider timeout")
        }
        fixture.pay(fixture.createPayment(), 20_000)
        fixture.pay(fixture.createPayment(), 20_000)

        assertEquals(2, fixture.requests.size)
        assertEquals(2.0, fixture.submittedCount())
        assertEquals(1.0, fixture.completedCount("timeout"))
        assertEquals(1.0, fixture.completedCount("success"))
        assertEquals(2, fixture.events.filterIsInstance<PaymentSubmittedEvent>().size)
        assertEquals(2, fixture.events.filterIsInstance<PaymentProcessedEvent>().size)
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

    private class Fixture(rate: Int = 1000, onRequest: (Request) -> Unit = {}) {
        val clock = MutableClock()
        val requests = CopyOnWriteArrayList<Request>()
        val events = CopyOnWriteArrayList<Event<PaymentAggregate>>()
        private val states = ConcurrentHashMap<UUID, PaymentAggregateState>()
        private val registry = SimpleMeterRegistry()

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
            val response = """{"transactionId":"${request.url.queryParameter("transactionId")}","paymentId":"${request.url.queryParameter("paymentId")}","result":true}"""
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

        fun createPayment(): UUID {
            val id = UUID.randomUUID()
            states[id] = PaymentAggregateState().apply {
                paymentCreatedApply(PaymentCreatedEvent(id, UUID.randomUUID(), 100, clock.millis()))
            }
            return id
        }

        fun pay(id: UUID, deadline: Long) = adapter.performPaymentAsync(id, 100, clock.millis(), deadline)
        fun submittedCount() = registry.get("shop.payment.submitted").counter().count()
        fun completedCount(result: String) = registry.get("shop.payment.completed").tag("result", result).counter().count()
    }
}
