package ru.quipy.payments.logic.payment.service

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*

@Service
class PaymentServiceImpl(
    private val paymentAccounts: List<PaymentExternalServiceAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentServiceImpl::class.java)
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, attemptNumber: Int, accountsToRetry: Set<String>?): Set<String> {
        val retryAccounts = mutableSetOf<String>()
        for (account in paymentAccounts) {
            val accountName = account.name()
            if (accountsToRetry != null && accountName !in accountsToRetry) continue

            val completed = account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline, attemptNumber)
            if (!completed) retryAccounts.add(accountName)
        }
        return retryAccounts
    }

    override fun totalThroughputPerMs(): Double {
        return paymentAccounts
            .filter { it.isEnabled() }
            .sumOf { it.throughputPerMs() }
    }

    override fun averageProcessingTimeMs(): Long {
        val accounts = paymentAccounts.filter { it.isEnabled() }
        if (accounts.isEmpty()) return 1000L
        return accounts.maxOf { it.averageProcessingTimeMs() }
    }
}