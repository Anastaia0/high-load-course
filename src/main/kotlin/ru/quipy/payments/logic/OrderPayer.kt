package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PostConstruct
import java.util.*
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.PaymentTask
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired private lateinit var paymentService: PaymentService

    @Autowired private lateinit var meterRegistry: MeterRegistry

    @Value("\${payment.service-name}") private lateinit var serviceName: String

    private lateinit var receivedPayments: Counter
    private lateinit var completedPayments: Counter

    @PostConstruct
    fun registerMetrics() {
        receivedPayments =
            Counter.builder("shop.payment.received")
                .description("Incoming payment requests before the queue")
                .tag("service", serviceName)
                .register(meterRegistry)
        completedPayments =
            Counter.builder("shop.payment.task.completed")
                .description("Payment tasks finished locally, including failures and deadline rejections")
                .tag("service", serviceName)
                .register(meterRegistry)

        io.micrometer.core.instrument.Gauge.builder("shop.payment.queue.size", paymentExecutor) { it.queue.size.toDouble() }
            .description("Payment tasks waiting in the local executor queue")
            .tag("service", serviceName)
            .register(meterRegistry)
    }

    private val sequence = AtomicLong()
    private val admissionSlots = Semaphore(10, true)

    fun tryReservePayment(): Boolean = admissionSlots.tryAcquire()

    fun releasePaymentReservation() {
        admissionSlots.release()
    }

    private val paymentExecutor =
        ThreadPoolExecutor(
            5,
            5,
            0L,
            TimeUnit.MILLISECONDS,
            PriorityBlockingQueue<Runnable>(),
            NamedThreadFactory("payment-submission-executor"),
            CallerBlockingRejectedExecutionHandler()
        )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        receivedPayments.increment()

        paymentExecutor.execute(
            PaymentTask(
                amount = amount,
                deadline = deadline,
                sequence = sequence.incrementAndGet()
            ) {
                try {
                    val createdEvent = paymentESService.create { it.create(paymentId, orderId, amount) }

                    logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                    paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
                } catch (e: Exception) {
                    logger.error("Payment task failed for payment $paymentId", e)
                } finally {
                    try {
                        completedPayments.increment()
                    } finally {
                        releasePaymentReservation()
                    }
                }
            }
        )

        return createdAt
    }
}
