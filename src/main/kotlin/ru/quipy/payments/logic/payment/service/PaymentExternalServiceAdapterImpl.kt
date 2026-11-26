package ru.quipy.payments.logic.payment.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import okhttp3.ConnectionPool
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.ratelimiter.implementation.SlidingWindowRateLimiter
import ru.quipy.common.utils.window.OngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.PaymentAggregateState
import ru.quipy.payments.logic.entities.ExternalSysResponse
import ru.quipy.payments.logic.payment.entities.PaymentAccountProperties
import java.io.InterruptedIOException
import java.net.HttpURLConnection.HTTP_CLIENT_TIMEOUT
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

class PaymentExternalServiceAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
) : PaymentExternalServiceAdapter {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        fun now() = System.currentTimeMillis()

        private const val RATE_LIMIT_BACKOFF_MS = 200L
        private const val RETRY_BACKOFF_MS = 50L
        private const val MAX_TEMPORARY_ERROR_RETRIES = 3
        private const val PROVIDER_TIMEOUT_FACTOR = 2L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val maxCallMillis = requestAverageProcessingTime.multipliedBy(PROVIDER_TIMEOUT_FACTOR)
        .toMillis().coerceIn(1L, Int.MAX_VALUE.toLong())
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(
        rate = rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1),
    )

    private val parallelWindow = OngoingWindow(parallelRequests)

    private val httpClient = OkHttpClient.Builder()
        .connectTimeout(Duration.ofSeconds(5))
        .readTimeout(maxCallMillis, TimeUnit.MILLISECONDS)
        .writeTimeout(Duration.ofSeconds(10))
        .retryOnConnectionFailure(false)
        .connectionPool(ConnectionPool(50, 5, TimeUnit.MINUTES))
        .build()

    private val accountTag = Tag.of("account", accountName)

    private val incomingPaymentsCounter: Counter = Counter.builder("payment_incoming_total")
        .tag("account", accountName)
        .tag("service", serviceName)
        .description("Total number of incoming payment requests")
        .register(meterRegistry)

    private val outgoingRequestsCounter: Counter = Counter.builder("payment_outgoing_total")
        .tag("account", accountName)
        .tag("service", serviceName)
        .description("Total number of outgoing HTTP requests to the payment provider")
        .register(meterRegistry)

    private val succeededPaymentsCounter: Counter = Counter.builder("payment_succeeded_total")
        .tag("account", accountName)
        .tag("service", serviceName)
        .description("Total number of successfully processed payments")
        .register(meterRegistry)



    private val parallelWaitCounter: Counter = Counter.builder("payment_parallel_wait_total")
        .tag("account", accountName)
        .tag("service", serviceName)
        .description("Total number of attempts to acquire a parallel window slot")
        .register(meterRegistry)

    private val parallelTimeoutCounter: Counter = Counter.builder("payment_parallel_timeout_total")
        .tag("account", accountName)
        .tag("service", serviceName)
        .description("Total number of failed attempts to acquire a parallel window slot")
        .register(meterRegistry)

    private val rateWaitCounter: Counter = Counter.builder("payment_rate_wait_total")
        .tag("account", accountName)
        .tag("service", serviceName)
        .description("Total number of attempts to acquire a rate limiter permit")
        .register(meterRegistry)

    private val rateTimeoutCounter: Counter = Counter.builder("payment_rate_timeout_total")
        .tag("account", accountName)
        .tag("service", serviceName)
        .description("Total number of failed attempts to acquire a rate limiter permit")
        .register(meterRegistry)

    private val temporaryErrorRetryCounter: Counter = Counter.builder("payment_temporary_retry_total")
        .tag("account", accountName)
        .tag("service", serviceName)
        .description("Total number of retries requested after a temporary provider error")
        .register(meterRegistry)

    private val timeoutRetryCounter = Counter.builder("payment_timeout_retry_total")
        .tag("account", accountName)
        .tag("service", serviceName)
        .description("Provider calls retried after HTTP 408 or a client timeout")
        .register(meterRegistry)

    private val providerCallDuration = Timer.builder("payment_provider_call_duration")
        .tag("account", accountName)
        .tag("service", serviceName)
        .description("Time spent on one HTTP call to the payment provider")
        .publishPercentiles(0.5, 0.9, 0.95, 0.99)
        .register(meterRegistry)

    init {
        meterRegistry.gauge(
            "payment_parallel_window_queue_size",
            listOf(accountTag),
            parallelWindow,
        ) { it.awaitingQueueSize().toDouble() }

        meterRegistry.gauge(
            "payment_parallel_window_available_slots",
            listOf(accountTag),
            parallelWindow,
        ) { it.availableSlots().toDouble() }

        meterRegistry.gauge(
            "payment_rate_limiter_current_count",
            listOf(accountTag),
            rateLimiter,
        ) { it.currentCount().toDouble() }
    }

    override fun name() = properties.accountName
    override fun isEnabled() = properties.enabled
    override fun price() = properties.price

    override fun performPaymentAsync(paymentId: UUID,amount: Int,paymentStartedAt: Long,deadline: Long,attemptNumber: Int): Boolean {
        if (attemptNumber == 0) incomingPaymentsCounter.increment()

        logger.warn("[$accountName] Submitting payment request for payment $paymentId (attempt=${attemptNumber + 1})")

        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        var lastFailureReason: String? = null
        var retryAfterTimeout = false
        val paymentUrl = "http://$paymentProviderHostPort/external/process".toHttpUrl().newBuilder()
            .addQueryParameter("serviceName", serviceName)
            .addQueryParameter("token", token)
            .addQueryParameter("accountName", accountName)
            .addQueryParameter("transactionId", transactionId.toString())
            .addQueryParameter("paymentId", paymentId.toString())
            .addQueryParameter("amount", amount.toString())
            .build()

        while (true) {
            val nowMs = now()
            val remaining = deadline - nowMs
            if (remaining <= 0) {
                val reason = lastFailureReason ?: "Deadline exceeded"
                logger.warn("[$accountName] Deadline exceeded for payment $paymentId, reason=$reason")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = reason)
                }
                return true
            }

            val permitWaitTimeout = Duration.ofMillis(remaining)
            parallelWaitCounter.increment()
            if (!parallelWindow.tryAcquire(permitWaitTimeout)) {
                logger.warn("[$accountName] Parallel limit wait timeout for payment $paymentId, will retry")
                parallelTimeoutCounter.increment()
                Thread.sleep(RETRY_BACKOFF_MS)
                continue
            }

            try {
                val remainingForRateLimit = deadline - now()
                if (remainingForRateLimit <= 0) continue

                rateWaitCounter.increment()
                if (!rateLimiter.tickBlocking(Duration.ofMillis(remainingForRateLimit))) {
                    logger.warn("[$accountName] Rate limit wait timeout for payment $paymentId")
                    rateTimeoutCounter.increment()
                    continue
                }

                val timeoutMillis = minOf(maxCallMillis, deadline - now())
                if (timeoutMillis <= 0) continue
                if (retryAfterTimeout) {
                    timeoutRetryCounter.increment()
                    retryAfterTimeout = false
                }

                val (status, rawBody) = callProvider(paymentUrl, timeoutMillis)
                if (status == HTTP_CLIENT_TIMEOUT) {
                    lastFailureReason = "Provider timeout (HTTP 408)"
                    retryAfterTimeout = true
                    logger.warn("[$accountName] Provider timed out for txId: $transactionId, payment: $paymentId")
                    continue
                }

                val providerResponse = try {
                    mapper.readValue(rawBody, ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] Parse failure for txId: $transactionId, payment: $paymentId, code: $status", e)
                    lastFailureReason = e.message ?: "Parse error"
                    continue
                }

                if (providerResponse.result) {
                    succeededPaymentsCounter.increment()
                    logger.info("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: true, message: ${providerResponse.message}")
                    paymentESService.update(paymentId) {
                        it.logProcessing(true, now(), transactionId, reason = providerResponse.message)
                    }
                    return true
                }

                val failureReason = providerResponse.message
                lastFailureReason = failureReason
                if (failureReason?.contains("Rate limit", ignoreCase = true) == true) {
                    logger.warn("[$accountName] Rate limit for payment $paymentId: $failureReason")
                    Thread.sleep(RATE_LIMIT_BACKOFF_MS)
                    continue
                }

                if (failureReason?.contains("Temporary error", ignoreCase = true) == true &&
                    attemptNumber < MAX_TEMPORARY_ERROR_RETRIES && now() < deadline
                ) {
                    temporaryErrorRetryCounter.increment()
                    logger.warn("[$accountName] Temporary error for payment $paymentId: $failureReason. Requesting retry.")
                    return false
                }

                logger.warn("[$accountName] Payment $paymentId failed: $failureReason")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = failureReason)
                }
                return true
            } catch (e: InterruptedIOException) {
                lastFailureReason = "Request timeout"
                retryAfterTimeout = true
                logger.warn("[$accountName] Request timed out for txId: $transactionId, payment: $paymentId")
            } catch (e: Exception) {
                lastFailureReason = e.message ?: "Request error"
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            } finally {
                parallelWindow.release()
            }
        }
    }

    private fun callProvider(paymentUrl: HttpUrl, timeoutMillis: Long): Pair<Int, String?> {
        val url = paymentUrl.newBuilder()
            .addQueryParameter("timeout", Duration.ofMillis(timeoutMillis).toString())
            .build()
        val call = httpClient.newCall(Request.Builder().url(url).post(emptyBody).build())
        call.timeout().timeout(timeoutMillis, TimeUnit.MILLISECONDS)

        outgoingRequestsCounter.increment()
        val sample = Timer.start(meterRegistry)
        try {
            return call.execute().use { response -> response.code to response.body?.string() }
        } finally {
            sample.stop(providerCallDuration)
        }
    }

    override fun throughputPerMs(): Double {
        val accountThroughputRps = minOf(
            rateLimitPerSec.toDouble(),
            parallelRequests.toDouble() / requestAverageProcessingTime.toMillis() * 1000.0
        )
        return accountThroughputRps / 1000.0
    }
    override fun averageProcessingTimeMs(): Long = requestAverageProcessingTime.toMillis()
}
