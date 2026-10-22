package ru.quipy.payments.logic

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import ru.quipy.payments.api.PaymentCreatedEvent

@Timeout(10)
class OrderPayerTest {
    @Test
    fun `bounded admission preserves accepted work and shutdown stops new payments`() {
        val service = BlockingPaymentService()
        val metrics = SimpleMeterRegistry()
        val events = CopyOnWriteArrayList<Any>()
        val payer = OrderPayer(paymentEventStore(events), service, metrics, "test", 2)
        val first = UUID.randomUUID()
        val cheap = UUID.randomUUID()
        val expensive = UUID.randomUUID()
        val rejected = UUID.randomUUID()
        val deadline = now() + 30_000
        try {
            payer.processPayment(UUID.randomUUID(), 1, first, deadline)
            assertEquals(first, service.received.poll(2, TimeUnit.SECONDS))
            payer.processPayment(UUID.randomUUID(), 10, cheap, deadline)
            payer.processPayment(UUID.randomUUID(), 100, expensive, deadline)
            assertEquals(2.0, metrics.get("shop.payment.queue.size").gauge().value())
            assertTimeout(Duration.ofMillis(250)) {
                assertThrows(RejectedExecutionException::class.java) {
                    payer.processPayment(UUID.randomUUID(), 1000, rejected, deadline)
                }
            }
            payer.shutdown()
            assertThrows(RejectedExecutionException::class.java) {
                payer.processPayment(UUID.randomUUID(), 1000, UUID.randomUUID(), deadline)
            }
            service.release.countDown()
            assertEquals(expensive, service.received.poll(2, TimeUnit.SECONDS))
            assertEquals(cheap, service.received.poll(2, TimeUnit.SECONDS))
            awaitCompleted(metrics, 3.0)
            assertEquals(setOf(first, cheap, expensive), events.filterIsInstance<PaymentCreatedEvent>().map { it.paymentId }.toSet())
            assertEquals(2.0, metrics.get("shop.payment.admission.rejected").counter().count())
            assertEquals(0.0, metrics.get("shop.payment.queue.size").gauge().value())
        } finally {
            service.release.countDown()
            payer.shutdown()
        }
    }

    @Test
    fun `a failing task does not prevent subsequent payments from running`() {
        val received = LinkedBlockingQueue<UUID>()
        val service = object : PaymentService {
            override fun workerCount() = 1
            override fun latestStart(deadline: Long) = deadline
            override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
                received.add(paymentId)
                if (amount == 1) throw IllegalStateException("test failure")
            }
        }
        val metrics = SimpleMeterRegistry()
        val payer = OrderPayer(paymentEventStore(), service, metrics, "test", 2)
        try {
            payer.processPayment(UUID.randomUUID(), 1, UUID.randomUUID(), now() + 10_000)
            assertNotNull(received.poll(2, TimeUnit.SECONDS))
            awaitCompleted(metrics, 1.0)
            payer.processPayment(UUID.randomUUID(), 2, UUID.randomUUID(), now() + 10_000)
            assertNotNull(received.poll(2, TimeUnit.SECONDS))
            awaitCompleted(metrics, 2.0)
            assertEquals(1.0, metrics.get("shop.payment.dispatch.errors").counter().count())
        } finally {
            payer.shutdown()
        }
    }

    private fun awaitCompleted(metrics: SimpleMeterRegistry, count: Double) {
        val until = System.nanoTime() + TimeUnit.SECONDS.toNanos(2)
        while (metrics.get("shop.payment.task.completed").counter().count() < count && System.nanoTime() < until) {
            Thread.sleep(5)
        }
        assertEquals(count, metrics.get("shop.payment.task.completed").counter().count())
    }
}

internal class BlockingPaymentService : PaymentService {
    val received = LinkedBlockingQueue<UUID>()
    val release = CountDownLatch(1)
    override fun workerCount() = 1
    override fun latestStart(deadline: Long) = deadline
    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        received.add(paymentId)
        check(release.await(5, TimeUnit.SECONDS))
    }
}
