package ru.quipy.payments.logic

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@Timeout(30)
class PaymentBatcherTest {
    @Test
    fun `a full batch is sent immediately without waiting for the timer`() {
        val captured = CopyOnWriteArrayList<List<PaymentToProcess>>()
        val sent = CountDownLatch(1)
        val payments = List(5) { payment() }
        PaymentBatcher(maxWait = Duration.ofSeconds(20)) {
            captured.add(it)
            sent.countDown()
        }.use { batcher ->
            payments.forEach(batcher::submit)
            assertTrue(sent.await(5, TimeUnit.SECONDS))
        }
        assertEquals(listOf(payments), captured.toList())
    }

    @Test
    fun `a partial batch is sent when the bounded wait expires`() {
        val captured = CopyOnWriteArrayList<List<PaymentToProcess>>()
        val sent = CountDownLatch(1)
        val payments = List(2) { payment() }
        PaymentBatcher(maxWait = Duration.ofMillis(50)) {
            captured.add(it)
            sent.countDown()
        }.use { batcher ->
            payments.forEach(batcher::submit)
            assertTrue(sent.await(5, TimeUnit.SECONDS))
        }
        assertEquals(listOf(payments), captured.toList())
    }

    @Test
    fun `shutdown drains the remaining payments exactly once and rejects new submissions`() {
        val captured = CopyOnWriteArrayList<List<PaymentToProcess>>()
        val payments = List(12) { payment() }
        val batcher = PaymentBatcher(maxWait = Duration.ofSeconds(20)) { captured.add(it) }
        payments.forEach(batcher::submit)
        batcher.close()
        batcher.close()

        assertEquals(listOf(2, 5, 5), captured.map { it.size }.sorted())
        assertEquals(12, captured.flatten().size)
        assertEquals(payments.toSet(), captured.flatten().toSet())
        assertThrows(IllegalStateException::class.java) { batcher.submit(payment()) }
    }

    private fun payment(): PaymentToProcess {
        val now = System.currentTimeMillis()
        return PaymentToProcess(UUID.randomUUID(), 100, now, now + 60_000)
    }
}
