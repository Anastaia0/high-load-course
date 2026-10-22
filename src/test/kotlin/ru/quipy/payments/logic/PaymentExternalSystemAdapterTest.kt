package ru.quipy.payments.logic

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.payments.api.PaymentProcessedEvent
import ru.quipy.payments.api.PaymentSubmittedEvent

@Timeout(15)
class PaymentExternalSystemAdapterTest {
    @Test
    fun `late response keeps provider capacity occupied and preserves the actual result`() {
        val events = CopyOnWriteArrayList<Any>()
        val store = paymentEventStore(events)
        val firstId = createPayment(store)
        val secondId = createPayment(store)
        val finishFirst = CountDownLatch(1)
        val calls = Executors.newFixedThreadPool(2, NamedThreadFactory("adapter-test"))
        val metrics = SimpleMeterRegistry()
        TestPaymentProvider {
            if (it == 1) check(finishFirst.await(10, TimeUnit.SECONDS))
            TestPaymentProvider.Reply()
        }.use { provider ->
            val adapter = PaymentExternalSystemAdapterImpl(account(), store, provider.address, "test-token", metrics)
            try {
                val deadline = now() + 1000
                val first = calls.submit { adapter.performPaymentAsync(firstId, 400, now(), deadline) }
                assertEquals(firstId, provider.received.poll(3, TimeUnit.SECONDS))
                Thread.sleep((deadline - now() + 100).coerceAtLeast(0))
                val secondStarted = CountDownLatch(1)
                val second = calls.submit {
                    secondStarted.countDown()
                    adapter.performPaymentAsync(secondId, 400, now(), now() + 5000)
                }
                assertTrue(secondStarted.await(1, TimeUnit.SECONDS))
                assertFalse(first.isDone)
                assertNull(provider.received.poll(150, TimeUnit.MILLISECONDS))
                assertEquals(1.0, metrics.get("shop.payment.provider.inflight").gauge().value())

                finishFirst.countDown()
                first.get(3, TimeUnit.SECONDS)
                assertEquals(secondId, provider.received.poll(3, TimeUnit.SECONDS))
                second.get(3, TimeUnit.SECONDS)
                assertEquals(1, provider.maxActive.get())
                val results = events.filterIsInstance<PaymentProcessedEvent>()
                assertEquals(2, results.size)
                assertTrue(results.all { it.success }, "Provider success remains success even after the client deadline")
                assertEquals(1.0, attempts(metrics, "success_after_deadline"))
                assertEquals(1.0, attempts(metrics, "success_on_time"))
                assertEquals(0.0, metrics.get("shop.payment.provider.inflight").gauge().value())
            } finally {
                finishFirst.countDown()
                calls.shutdownNow()
            }
        }
    }

    @Test
    fun `waiting payment expires without entering an occupied provider slot`() {
        val events = CopyOnWriteArrayList<Any>()
        val store = paymentEventStore(events)
        val firstId = createPayment(store)
        val secondId = createPayment(store)
        val finishFirst = CountDownLatch(1)
        val calls = Executors.newFixedThreadPool(2, NamedThreadFactory("adapter-test"))
        val metrics = SimpleMeterRegistry()
        TestPaymentProvider {
            if (it == 1) check(finishFirst.await(10, TimeUnit.SECONDS))
            TestPaymentProvider.Reply()
        }.use { provider ->
            val adapter = PaymentExternalSystemAdapterImpl(account(), store, provider.address, "test-token", metrics)
            try {
                val first = calls.submit { adapter.performPaymentAsync(firstId, 400, now(), now() + 10_000) }
                assertEquals(firstId, provider.received.poll(3, TimeUnit.SECONDS))
                val second = calls.submit { adapter.performPaymentAsync(secondId, 400, now(), now() + 150) }
                second.get(2, TimeUnit.SECONDS)
                assertFalse(first.isDone)
                assertNull(provider.received.poll(50, TimeUnit.MILLISECONDS))
                assertFalse(events.filterIsInstance<PaymentSubmittedEvent>().single { it.paymentId == secondId }.success)
                assertFalse(events.filterIsInstance<PaymentProcessedEvent>().single().success)
                assertEquals(1.0, attempts(metrics, "expired_before_send"))
                finishFirst.countDown()
                first.get(3, TimeUnit.SECONDS)
            } finally {
                finishFirst.countDown()
                calls.shutdownNow()
            }
        }
    }

    @Test
    fun `expired payment records submission and processing without an HTTP call`() {
        val events = CopyOnWriteArrayList<Any>()
        val store = paymentEventStore(events)
        val paymentId = createPayment(store)
        val metrics = SimpleMeterRegistry()
        TestPaymentProvider().use { provider ->
            val adapter = PaymentExternalSystemAdapterImpl(account(), store, provider.address, "test-token", metrics)
            adapter.performPaymentAsync(paymentId, 400, now(), now())
            assertNull(provider.received.poll())
            assertFalse(events.filterIsInstance<PaymentSubmittedEvent>().single().success)
            assertFalse(events.filterIsInstance<PaymentProcessedEvent>().single().success)
            assertEquals(1.0, attempts(metrics, "expired_before_send"))
        }
    }

    @Test
    fun `rate limit waiting expires and releases capacity for the next payment`() {
        val events = CopyOnWriteArrayList<Any>()
        val store = paymentEventStore(events)
        val ids = List(3) { createPayment(store) }
        val metrics = SimpleMeterRegistry()
        TestPaymentProvider().use { provider ->
            val adapter = PaymentExternalSystemAdapterImpl(
                account().copy(rateLimitPerSec = 1), store, provider.address, "test-token", metrics
            )
            adapter.performPaymentAsync(ids[0], 400, now(), now() + 3000)
            assertEquals(ids[0], provider.received.poll())
            adapter.performPaymentAsync(ids[1], 400, now(), now() + 100)
            assertNull(provider.received.poll())
            adapter.performPaymentAsync(ids[2], 400, now(), now() + 3000)
            assertEquals(ids[2], provider.received.poll())
            val results = events.filterIsInstance<PaymentProcessedEvent>()
            assertEquals(listOf(true, false, true), ids.map { id -> results.single { it.paymentId == id }.success })
            assertEquals(1.0, attempts(metrics, "expired_before_send"))
            assertEquals(1, provider.maxActive.get())
        }
    }

    @Test
    fun `provider failure and invalid response release capacity and record results`() {
        val events = CopyOnWriteArrayList<Any>()
        val store = paymentEventStore(events)
        val ids = List(3) { createPayment(store) }
        val metrics = SimpleMeterRegistry()
        TestPaymentProvider {
            when (it) {
                1 -> TestPaymentProvider.Reply(success = false, status = 503)
                2 -> TestPaymentProvider.Reply(malformed = true)
                else -> TestPaymentProvider.Reply()
            }
        }.use { provider ->
            val adapter = PaymentExternalSystemAdapterImpl(account(), store, provider.address, "test-token", metrics)
            ids.forEach { adapter.performPaymentAsync(it, 400, now(), now() + 5000) }
            assertEquals(listOf(false, false, true), events.filterIsInstance<PaymentProcessedEvent>().map { it.success })
            assertEquals(3, events.filterIsInstance<PaymentSubmittedEvent>().size)
            assertEquals(1.0, attempts(metrics, "provider_failure"))
            assertEquals(1.0, attempts(metrics, "provider_error_unknown"))
            assertEquals(0.0, metrics.get("shop.payment.provider.inflight").gauge().value())
        }
    }

    @Test
    fun `transport failure records an unknown outcome and releases the provider slot`() {
        val events = CopyOnWriteArrayList<Any>()
        val store = paymentEventStore(events)
        val ids = List(2) { createPayment(store) }
        val metrics = SimpleMeterRegistry()
        val provider = TestPaymentProvider()
        val address = provider.address
        provider.close()
        val adapter = PaymentExternalSystemAdapterImpl(account(), store, address, "test-token", metrics)
        ids.forEach { adapter.performPaymentAsync(it, 400, now(), now() + 5000) }
        val results = events.filterIsInstance<PaymentProcessedEvent>()
        assertEquals(2, results.size)
        assertTrue(results.all { !it.success })
        assertEquals(2.0, attempts(metrics, "provider_error_unknown"))
        assertEquals(0.0, metrics.get("shop.payment.provider.inflight").gauge().value())
    }

    @Test
    fun `worker capacity respects rate processing time and parallel limit`() {
        val store = paymentEventStore()
        fun capacity(parallel: Int, rate: Int, millis: Long): Int = PaymentExternalSystemAdapterImpl(
            account().copy(parallelRequests = parallel, rateLimitPerSec = rate, averageProcessingTime = Duration.ofMillis(millis)),
            store, "127.0.0.1:1", "test-token", SimpleMeterRegistry()
        ).parallelCapacity()
        assertEquals(5, capacity(5, 100, 1000))
        assertEquals(4, capacity(100, 10, 250))
        assertEquals(1, capacity(100, 10, 0))
    }

    private fun account() = PaymentAccountProperties(
        serviceName = "test", accountName = "acc-18", parallelRequests = 1,
        rateLimitPerSec = 1000, price = 30, averageProcessingTime = Duration.ofMillis(10), enabled = true,
    )

    private fun attempts(metrics: SimpleMeterRegistry, outcome: String) =
        metrics.get("shop.payment.attempts").tag("outcome", outcome).counter().count()

    private fun createPayment(
        store: ru.quipy.core.EventSourcingService<UUID, ru.quipy.payments.api.PaymentAggregate, PaymentAggregateState>,
    ): UUID = UUID.randomUUID().also { id -> store.create { it.create(id, UUID.randomUUID(), 400) } }
}
