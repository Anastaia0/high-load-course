package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
        private val properties: PaymentAccountProperties,
        private val paymentESService:
                EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
        private val paymentProviderHostPort: String,
        private val token: String,
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

    // Space requests evenly, leaving a small margin for scheduling and network jitter.
    private val rateLimiter =
            SlidingWindowRateLimiter(1, Duration.ofMillis(1000L / rateLimitPerSec + 10))
    private val paymentSemaphore = Semaphore(parallelRequests, true)
    private val client =
            OkHttpClient.Builder()
                    .readTimeout(0, TimeUnit.MILLISECONDS)
                    .retryOnConnectionFailure(false)
                    .build()

    override fun performPaymentAsync(
            paymentId: UUID,
            amount: Int,
            paymentStartedAt: Long,
            deadline: Long
    ) {
        val latestStart = deadline - requestAverageProcessingTime.toMillis()

        // Ждём разрешение частоты только пока остаётся время.
        try {
            while (true) {
                if (now() >= latestStart) {
                    rejectPayment(
                            paymentId,
                            paymentStartedAt,
                            "Not enough time before payment deadline."
                    )
                    return
                }

                if (rateLimiter.tick()) {
                    break
                }

                Thread.sleep(10)
            }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()

            rejectPayment(paymentId, paymentStartedAt, "Interrupted while waiting for rate limit.")
            return
        }

        val acquired =
                try {
                    val remainingMillis = latestStart - now()

                    remainingMillis > 0 &&
                            paymentSemaphore.tryAcquire(remainingMillis, TimeUnit.MILLISECONDS)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()

                    rejectPayment(
                            paymentId,
                            paymentStartedAt,
                            "Interrupted while waiting for provider slot."
                    )
                    return
                }

        if (!acquired) {
            rejectPayment(
                    paymentId,
                    paymentStartedAt,
                    "Deadline exceeded while waiting for provider slot."
            )
            return
        }

        try {
            if (now() >= latestStart) {
                rejectPayment(
                        paymentId,
                        paymentStartedAt,
                        "Not enough time after waiting for provider slot."
                )
                return
            }

            performPayment(paymentId, amount, paymentStartedAt, latestStart)
        } finally {
            paymentSemaphore.release()
        }
    }

    private fun performPayment(
            paymentId: UUID,
            amount: Int,
            paymentStartedAt: Long,
            latestStart: Long
    ) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом
        // тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(
                    success = true,
                    transactionId,
                    now(),
                    Duration.ofMillis(now() - paymentStartedAt)
            )
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        try {
            val request =
                    Request.Builder()
                            .run {
                                url(
                                        "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
                                )
                                post(emptyBody)
                            }
                            .build()

            // Создание запроса и запись submission тоже занимают время.
            if (now() >= latestStart) {
                paymentESService.update(paymentId) {
                    it.logProcessing(
                            false,
                            now(),
                            transactionId,
                            reason = "Not enough time before HTTP call."
                    )
                }
                return
            }

            val sendingAt = now()
            val paymentDeadline = latestStart + requestAverageProcessingTime.toMillis()

            logger.info(
                    "[$accountName] Sending payment: paymentId=$paymentId, " +
                            "waitBeforeHttpMs=${sendingAt - paymentStartedAt}, " +
                            "remainingBeforeDeadlineMs=${paymentDeadline - sendingAt}"
            )

            val remainingMillis = paymentDeadline - now()

            if (remainingMillis <= 0L) {
                paymentESService.update(paymentId) {
                    it.logProcessing(
                            false,
                            now(),
                            transactionId,
                            reason = "Payment deadline exceeded before HTTP call."
                    )
                }
                return
            }

            val call = client.newCall(request)
            call.timeout().timeout(remainingMillis, TimeUnit.MILLISECONDS)

            call.execute().use { response ->
                val rawBody = response.body?.string()

                val body =
                        try {
                            mapper.readValue(rawBody, ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error(
                                    "[$accountName] Invalid provider response: " +
                                            "payment=$paymentId, HTTP=${response.code}",
                                    e
                            )

                            ExternalSysResponse(
                                    transactionId.toString(),
                                    paymentId.toString(),
                                    false,
                                    e.message
                            )
                        }

                logger.warn(
                        "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}"
                )

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных
                // оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная
                // ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error(
                            "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId",
                            e
                    )
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }
                else -> {
                    logger.error(
                            "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                            e
                    )

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun rejectPayment(paymentId: UUID, paymentStartedAt: Long, reason: String) {
        val transactionId = UUID.randomUUID()
        val rejectedAt = now()

        logger.warn(
                "[$accountName] Payment rejected locally: " +
                        "paymentId=$paymentId, " +
                        "elapsedMs=${rejectedAt - paymentStartedAt}, " +
                        "reason=$reason"
        )

        paymentESService.update(paymentId) {
            it.logSubmission(
                    false,
                    transactionId,
                    rejectedAt,
                    Duration.ofMillis(rejectedAt - paymentStartedAt)
            )
        }

        paymentESService.update(paymentId) {
            it.logProcessing(false, rejectedAt, transactionId, reason = reason)
        }
    }
}

public fun now() = System.currentTimeMillis()
