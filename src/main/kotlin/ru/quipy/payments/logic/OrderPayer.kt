package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PreDestroy
import java.util.UUID
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.PaymentPriorityQueue
import ru.quipy.common.utils.PaymentTask
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate

@Service
class OrderPayer(
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentService: PaymentService,
    meterRegistry: MeterRegistry,
    @Value("\${payment.service-name}") serviceName: String,
    @Value("\${payment.queue.capacity:110}") queueCapacity: Int,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    private val receivedPayments = Counter.builder("shop.payment.received")
        .description("Incoming payment requests before admission")
        .tag("service", serviceName).register(meterRegistry)
    private val completedPayments = Counter.builder("shop.payment.task.completed")
        .description("Payment tasks finished locally, including failures and deadline rejections")
        .tag("service", serviceName).register(meterRegistry)
    private val rejectedPayments = meterRegistry.counter("shop.payment.admission.rejected", "service", serviceName)
    private val dispatchErrors = meterRegistry.counter("shop.payment.dispatch.errors", "service", serviceName)
    private val sequence = AtomicLong()
    private val workers = paymentService.workerCount()
    private val paymentExecutor = ThreadPoolExecutor(
        workers, workers, 0L, TimeUnit.MILLISECONDS,
        PaymentPriorityQueue(queueCapacity),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.AbortPolicy(),
    )

    init {
        Gauge.builder("shop.payment.queue.size", paymentExecutor) { it.queue.size.toDouble() }
            .tag("service", serviceName).register(meterRegistry)
        Gauge.builder("shop.payment.workers.active", paymentExecutor) { it.activeCount.toDouble() }
            .tag("service", serviceName).register(meterRegistry)
        paymentExecutor.prestartAllCoreThreads()
    }

    @PreDestroy
    fun shutdown() = paymentExecutor.shutdown()

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = now()
        receivedPayments.increment()
        val task = PaymentTask(amount, paymentService.latestStart(deadline), sequence.incrementAndGet()) {
            try {
                paymentESService.create { it.create(paymentId, orderId, amount) }
                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            } catch (e: Exception) {
                dispatchErrors.increment()
                logger.error("Payment task failed for payment $paymentId", e)
            } finally {
                completedPayments.increment()
            }
        }
        try {
            paymentExecutor.execute(task)
        } catch (e: RejectedExecutionException) {
            rejectedPayments.increment()
            throw e
        }
        return createdAt
    }
}
