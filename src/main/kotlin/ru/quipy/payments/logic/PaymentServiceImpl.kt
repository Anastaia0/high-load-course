package ru.quipy.payments.logic

import java.util.UUID
import org.springframework.stereotype.Service

@Service
class PaymentSystemImpl(paymentAccounts: List<PaymentExternalSystemAdapter>) : PaymentService {
    private val account: PaymentExternalSystemAdapter

    init {
        val enabled = paymentAccounts.filter { it.isEnabled() }
        require(enabled.size == 1) { "Configure exactly one enabled payment account" }
        account = enabled.single()
    }

    override fun workerCount(): Int = account.parallelCapacity()

    override fun latestStart(deadline: Long): Long = account.latestStart(deadline)

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
    }
}
