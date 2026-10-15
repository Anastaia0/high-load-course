package ru.quipy.payments.logic

import org.springframework.beans.factory.DisposableBean
import org.springframework.stereotype.Service
import java.util.*


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService, DisposableBean {
    private val batcher = PaymentBatcher { payments ->
        for (account in paymentAccounts) {
            account.performPaymentsAsync(payments)
        }
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        batcher.submit(PaymentToProcess(paymentId, amount, paymentStartedAt, deadline))
    }

    override fun destroy() = batcher.close()
}
