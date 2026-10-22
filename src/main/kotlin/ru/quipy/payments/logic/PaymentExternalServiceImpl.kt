package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.ceil
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {
    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapterImpl::class.java)
        private val emptyBody = RequestBody.create(null, ByteArray(0))
        private val mapper = ObjectMapper().registerKotlinModule()
    }

    init {
        require(properties.parallelRequests > 0) { "Parallel request limit must be positive" }
        require(!properties.averageProcessingTime.isNegative) { "Average processing time cannot be negative" }
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimiter = SlidingWindowRateLimiter(properties.rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val paymentSemaphore = Semaphore(properties.parallelRequests, true)
    private val client = OkHttpClient.Builder()
        .readTimeout(0, TimeUnit.MILLISECONDS)
        .callTimeout(0, TimeUnit.MILLISECONDS)
        .retryOnConnectionFailure(false)
        .build()
    private val tags = arrayOf("service", serviceName, "account", accountName)
    private val inFlight = AtomicInteger()
    private val httpTimer = meterRegistry.timer("shop.payment.provider.duration", *tags)
    private val waitTimers = listOf("sent", "discarded").associateWith {
        meterRegistry.timer("shop.payment.queue.wait", *tags, "outcome", it)
    }
    private val outcomes = listOf(
        "expired_before_send", "interrupted", "success_on_time", "success_after_deadline",
        "provider_failure", "provider_error_unknown",
    ).associateWith { meterRegistry.counter("shop.payment.attempts", *tags, "outcome", it) }

    init {
        Gauge.builder("shop.payment.provider.inflight", inFlight) { it.get().toDouble() }
            .tags(*tags).register(meterRegistry)
        Gauge.builder("shop.payment.provider.waiters", paymentSemaphore) { it.queueLength.toDouble() }
            .tags(*tags).register(meterRegistry)
    }

    override fun parallelCapacity(): Int = minOf(
        properties.parallelRequests.toLong(),
        ceil(properties.rateLimitPerSec * properties.averageProcessingTime.toNanos() / 1_000_000_000.0)
            .toLong() + 1L,
    ).coerceAtLeast(1L).toInt()

    override fun latestStart(deadline: Long): Long = deadline - properties.averageProcessingTime.toMillis()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        val latestStart = latestStart(deadline)
        var acquired = false
        try {
            try {
                val remaining = latestStart - now()
                acquired = remaining > 0 && paymentSemaphore.tryAcquire(remaining, TimeUnit.MILLISECONDS)
                if (!acquired) {
                    rejectPayment(paymentId, transactionId, paymentStartedAt, "Deadline exceeded waiting for provider capacity.")
                    return
                }
                // Consume a rate permit only when provider capacity is available.
                if (!rateLimiter.tickBlocking(Duration.ofMillis(latestStart - now()))) {
                    rejectPayment(paymentId, transactionId, paymentStartedAt, "Deadline exceeded waiting for rate limit.")
                    return
                }
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                rejectPayment(paymentId, transactionId, paymentStartedAt, "Payment dispatch interrupted.", "interrupted")
                return
            }

            val request = Request.Builder()
                .url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                .post(emptyBody)
                .build()

            if (now() >= latestStart) {
                rejectPayment(paymentId, transactionId, paymentStartedAt, "Not enough time before payment deadline.")
                return
            }

            val submittedAt = now()
            paymentESService.update(paymentId) {
                it.logSubmission(true, transactionId, submittedAt, Duration.ofMillis(submittedAt - paymentStartedAt))
            }

            // Recording submission can use up the remaining dispatch budget.
            if (now() >= latestStart) {
                recordWait(paymentStartedAt, "discarded")
                outcomes.getValue("expired_before_send").increment()
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Not enough time before HTTP call.")
                }
                return
            }

            recordWait(paymentStartedAt, "sent")
            val result = callProvider(request, paymentId, transactionId)
            val processedAt = now()

            // A client deadline does not cancel processing at the provider. Keep the slot
            // until the HTTP call ends and its response has been consumed.
            paymentSemaphore.release()
            acquired = false

            val outcome = when {
                result.unknown -> "provider_error_unknown"
                !result.success -> "provider_failure"
                processedAt > deadline -> "success_after_deadline"
                else -> "success_on_time"
            }
            outcomes.getValue(outcome).increment()
            paymentESService.update(paymentId) {
                it.logProcessing(result.success, processedAt, transactionId, reason = result.message)
            }
        } finally {
            if (acquired) paymentSemaphore.release()
        }
    }

    private fun callProvider(request: Request, paymentId: UUID, transactionId: UUID): ProviderResult {
        val startedAt = System.nanoTime()
        inFlight.incrementAndGet()
        try {
            return client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string().orEmpty(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    return@use ProviderResult(false, "Invalid provider response (HTTP " + response.code + ").", true)
                }
                ProviderResult(response.isSuccessful && body.result, body.message)
            }
        } catch (e: Exception) {
            // Transport exceptions can contain the request URL with credentials.
            logger.warn("Provider call failed: payment=$paymentId, transaction=$transactionId, type=" + e.javaClass.simpleName)
            return ProviderResult(false, "Provider call failed: " + e.javaClass.simpleName, true)
        } finally {
            inFlight.decrementAndGet()
            httpTimer.record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS)
        }
    }

    private fun rejectPayment(
        paymentId: UUID,
        transactionId: UUID,
        paymentStartedAt: Long,
        reason: String,
        outcome: String = "expired_before_send",
    ) {
        val rejectedAt = now()
        recordWait(paymentStartedAt, "discarded")
        outcomes.getValue(outcome).increment()
        paymentESService.update(paymentId) {
            it.logSubmission(false, transactionId, rejectedAt, Duration.ofMillis(rejectedAt - paymentStartedAt))
        }
        paymentESService.update(paymentId) {
            it.logProcessing(false, rejectedAt, transactionId, reason = reason)
        }
    }

    private fun recordWait(startedAt: Long, outcome: String) {
        waitTimers.getValue(outcome).record(Duration.ofMillis((now() - startedAt).coerceAtLeast(0)))
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName

    private data class ProviderResult(val success: Boolean, val message: String?, val unknown: Boolean = false)
}

fun now() = System.currentTimeMillis()
