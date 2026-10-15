package ru.quipy.payments.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class PaymentMetrics(
    private val registry: MeterRegistry,
    @param:Value("\${payment.service-name}") private val serviceName: String,
) {
    private val received = Counter.builder("shop.payment.received")
        .description("Payment requests accepted by the shop, including time spent waiting for a worker.")
        .tag("service", serviceName)
        .register(registry)

    fun recordReceived() = received.increment()

    fun forAccount(accountName: String) = AccountPaymentMetrics(registry, serviceName, accountName)
}

class AccountPaymentMetrics(registry: MeterRegistry, serviceName: String, accountName: String) {
    private val submitted = Counter.builder("shop.payment.submitted")
        .description("Payments sent to the provider, including individual entries of bulk requests.")
        .tags("service", serviceName, "account", accountName)
        .register(registry)

    private val providerRequests = Counter.builder("shop.payment.provider.requests")
        .description("HTTP requests to the provider; one bulk request can contain several payments.")
        .tags("service", serviceName, "account", accountName)
        .register(registry)

    private val completed = PaymentResult.entries.associateWith { result ->
        Counter.builder("shop.payment.completed")
            .description("Payment attempts with a recorded processing outcome; not the customer's deadline result.")
            .tags("service", serviceName, "account", accountName, "result", result.name.lowercase())
            .register(registry)
    }

    fun recordSubmitted(count: Int = 1) = submitted.increment(count.toDouble())

    fun recordProviderRequest() = providerRequests.increment()

    fun recordCompleted(result: PaymentResult) = completed.getValue(result).increment()
}

enum class PaymentResult {
    SUCCESS,
    FAILED,
    TIMEOUT,
    ERROR,
    DEADLINE_EXCEEDED,
}
