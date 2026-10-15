package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

internal class PaymentBatcher(
    private val maxBatchSize: Int = 5,
    private val maxWait: Duration = Duration.ofSeconds(2),
    private val processBatch: (List<PaymentToProcess>) -> Unit,
) : AutoCloseable {
    private val logger = LoggerFactory.getLogger(PaymentBatcher::class.java)
    private val scheduler = Executors.newSingleThreadScheduledExecutor(NamedThreadFactory("payment-batch-timer"))
    private val workers = Executors.newFixedThreadPool(5, NamedThreadFactory("payment-batch-worker"))
    private val pending = mutableListOf<PaymentToProcess>()
    private var flushTask: ScheduledFuture<*>? = null
    private var generation = 0L
    private var closed = false

    init {
        require(maxBatchSize > 0)
        require(!maxWait.isNegative && !maxWait.isZero)
    }

    @Synchronized
    fun submit(payment: PaymentToProcess) {
        check(!closed) { "Payment batcher is closed" }
        pending.add(payment)
        if (pending.size == maxBatchSize) {
            dispatch()
        } else if (pending.size == 1) {
            val scheduledGeneration = generation
            flushTask = scheduler.schedule({ flush(scheduledGeneration) }, maxWait.toNanos(), TimeUnit.NANOSECONDS)
        }
    }

    @Synchronized
    private fun flush(scheduledGeneration: Long) {
        // A timer already waiting for this monitor must not flush a newer batch.
        if (scheduledGeneration == generation && pending.isNotEmpty()) dispatch()
    }

    private fun dispatch() {
        flushTask?.cancel(false)
        flushTask = null
        generation++
        val batch = pending.toList()
        pending.clear()
        workers.execute {
            try {
                processBatch(batch)
            } catch (error: Exception) {
                logger.error("Payment batch processing failed; size={}", batch.size, error)
            }
        }
    }

    override fun close() {
        synchronized(this) {
            if (closed) return
            closed = true
            if (pending.isNotEmpty()) dispatch()
            scheduler.shutdownNow()
            workers.shutdown()
        }
        try {
            if (!workers.awaitTermination(30, TimeUnit.SECONDS)) {
                logger.warn("Payment batches are still finishing during shutdown")
            }
        } catch (error: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }
}
