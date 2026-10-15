package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.metrics.AccountPaymentMetrics
import ru.quipy.payments.metrics.PaymentResult
import java.net.SocketTimeoutException
import java.time.Clock
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Semaphore

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val metrics: AccountPaymentMetrics,
    private val clock: Clock = Clock.systemUTC(),
    private val client: OkHttpClient = OkHttpClient.Builder().build(),
) : PaymentExternalSystemAdapter {
    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapterImpl::class.java)
        private val mapper = ObjectMapper().registerKotlinModule()
        private val jsonMediaType = "application/json".toMediaType()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    // A conservative admission reserve, not a bound on the entire HTTP call.
    private val processingBudgetMillis = maxOf(properties.averageProcessingTime.toMillis(), client.readTimeoutMillis.toLong())
    // The provider charges each payment, but applies both limits once per bulk HTTP request.
    private val rateLimiter = SlidingWindowRateLimiter(1, Duration.ofMillis(1000L / properties.rateLimitPerSec + 10))
    private val paymentSemaphore = Semaphore(properties.parallelRequests, true)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) =
        performPaymentsAsync(listOf(PaymentToProcess(paymentId, amount, paymentStartedAt, deadline)))

    override fun performPaymentsAsync(payments: List<PaymentToProcess>) {
        if (payments.isEmpty()) return
        paymentSemaphore.acquire()
        try {
            performPayments(payments)
        } finally {
            paymentSemaphore.release()
        }
    }

    private fun performPayments(payments: List<PaymentToProcess>) {
        val attempts = payments.map { Attempt(it) }.toMutableList()
        try {
            rejectExpired(attempts)
            if (attempts.isEmpty()) return
            rateLimiter.tickBlocking()
            rejectExpired(attempts)
            if (attempts.isEmpty()) return

            val bulk = ExternalSysBulkRequest(serviceName, accountName, attempts.map {
                ExternalSysPaymentRequest(it.transactionId.toString(), it.payment.paymentId.toString(), it.payment.amount)
            })
            val url = "http://$paymentProviderHostPort/external/process/bulk".toHttpUrl().newBuilder()
                .addQueryParameter("serviceName", serviceName)
                .addQueryParameter("token", token)
                .build()
            val request = Request.Builder().url(url)
                .put(mapper.writeValueAsString(bulk).toRequestBody(jsonMediaType)).build()
            attempts.forEach { recordSubmission(it, true) }

            metrics.recordSubmitted(attempts.size)
            metrics.recordProviderRequest()
            client.newCall(request).execute().use { response ->
                val body = mapper.readValue(response.body?.string(), ExternalSysBulkResponse::class.java)
                val expected = attempts.associate { it.payment.paymentId.toString() to it.transactionId.toString() }
                val results = body.responses.associateBy { it.paymentId }
                check(body.responses.size == attempts.size && results.keys == expected.keys &&
                    results.all { (paymentId, result) -> result.transactionId == expected[paymentId] }) {
                    "Invalid bulk response identifiers"
                }
                for (attempt in attempts) {
                    val result = results.getValue(attempt.payment.paymentId.toString())
                    val success = response.isSuccessful && result.result
                    recordOutcome(attempt, if (success) PaymentResult.SUCCESS else PaymentResult.FAILED,
                        result.message ?: if (response.isSuccessful) null else "Provider HTTP ${response.code}.")
                }
                logger.info("[{}] Bulk request completed; payments={}, status={}", accountName, attempts.size, response.code)
            }
        } catch (error: Exception) {
            val result = if (error is SocketTimeoutException) PaymentResult.TIMEOUT else PaymentResult.ERROR
            val reason = if (error is SocketTimeoutException) "Request timeout."
                else "Provider request failed: ${error.javaClass.simpleName}."
            // Transport failures are not retried: the provider may already have processed the payments.
            logger.warn("[{}] Bulk request failed; payments={}, type={}", accountName, attempts.size, error.javaClass.simpleName)
            for (attempt in attempts.filterNot { it.completed }) {
                if (!attempt.submissionRecorded) recordSubmission(attempt, false)
                recordOutcome(attempt, result, reason)
            }
            if (error is InterruptedException) Thread.currentThread().interrupt()
        }
    }

    private fun rejectExpired(attempts: MutableList<Attempt>) {
        val iterator = attempts.iterator()
        while (iterator.hasNext()) {
            val attempt = iterator.next()
            if (attempt.payment.deadline - clock.millis() <= processingBudgetMillis) {
                recordSubmission(attempt, false)
                recordOutcome(attempt, PaymentResult.DEADLINE_EXCEEDED, "Insufficient time before payment deadline.")
                iterator.remove()
            }
        }
    }

    private fun recordSubmission(attempt: Attempt, success: Boolean) {
        val submittedAt = clock.millis()
        paymentESService.update(attempt.payment.paymentId) {
            it.logSubmission(success, attempt.transactionId, submittedAt,
                Duration.ofMillis(submittedAt - attempt.payment.paymentStartedAt))
        }
        attempt.submissionRecorded = true
    }

    private fun recordOutcome(attempt: Attempt, result: PaymentResult, reason: String?) {
        paymentESService.update(attempt.payment.paymentId) {
            it.logProcessing(result == PaymentResult.SUCCESS, clock.millis(), attempt.transactionId, reason = reason)
        }
        attempt.completed = true
        metrics.recordCompleted(result)
    }

    private class Attempt(val payment: PaymentToProcess) {
        val transactionId: UUID = UUID.randomUUID()
        var submissionRecorded = false
        var completed = false
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}
