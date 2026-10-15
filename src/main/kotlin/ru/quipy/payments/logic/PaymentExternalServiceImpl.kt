package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.metrics.AccountPaymentMetrics
import ru.quipy.payments.metrics.PaymentResult
import java.net.SocketTimeoutException
import java.time.Clock
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore


// Advice: always treat time as a Duration
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
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    // A conservative admission reserve, not a bound on the entire HTTP call.
    private val processingBudgetMillis = maxOf(requestAverageProcessingTime.toMillis(), client.readTimeoutMillis.toLong())

    // Space requests evenly, leaving a small margin for scheduling and network jitter.
    private val rateLimiter = SlidingWindowRateLimiter(1, Duration.ofMillis(1000L / rateLimitPerSec + 10))
    private val paymentSemaphore = Semaphore(parallelRequests, true)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        paymentSemaphore.acquire()
        try {
            performPayment(paymentId, amount, paymentStartedAt, deadline)
        } finally {
            paymentSemaphore.release()
        }
    }

    private fun performPayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        var submissionRecorded = false

        // Bombardier needs a submission event for both sent and rejected attempts.
        fun recordSubmission(success: Boolean) {
            val submittedAt = clock.millis()
            paymentESService.update(paymentId) {
                it.logSubmission(success, transactionId, submittedAt, Duration.ofMillis(submittedAt - paymentStartedAt))
            }
            submissionRecorded = true
        }

        try {
            val hasTimeAfterSemaphore = hasTimeForProcessing(deadline)
            if (hasTimeAfterSemaphore) {
                rateLimiter.tickBlocking()
            }
            val shouldSubmit = hasTimeAfterSemaphore && hasTimeForProcessing(deadline)
            recordSubmission(shouldSubmit)
            if (!shouldSubmit) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, clock.millis(), transactionId, reason = "Insufficient time before payment deadline.")
                }
                metrics.recordCompleted(PaymentResult.DEADLINE_EXCEEDED)
                return
            }

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            val call = client.newCall(request)
            metrics.recordSubmitted()
            call.execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, clock.millis(), transactionId, reason = body.message)
                }
                metrics.recordCompleted(if (body.result) PaymentResult.SUCCESS else PaymentResult.FAILED)
            }
        } catch (e: Exception) {
            if (!submissionRecorded) {
                recordSubmission(false)
            }
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, clock.millis(), transactionId, reason = "Request timeout.")
                    }
                    metrics.recordCompleted(PaymentResult.TIMEOUT)
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, clock.millis(), transactionId, reason = e.message)
                    }
                    metrics.recordCompleted(PaymentResult.ERROR)
                }
            }
            if (e is InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }
    }

    private fun hasTimeForProcessing(deadline: Long): Boolean =
        deadline - clock.millis() > processingBudgetMillis

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}
