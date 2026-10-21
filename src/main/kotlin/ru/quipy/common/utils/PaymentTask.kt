package ru.quipy.common.utils

class PaymentTask(
    private val amount: Int,
    private val sequence: Long,
    private val action: () -> Unit
) : Runnable, Comparable<PaymentTask> {

    override fun run() = action()

    override fun compareTo(other: PaymentTask): Int {
        val amountComparison = other.amount.compareTo(amount)

        return if (amountComparison != 0) {
            amountComparison
        } else {
            other.sequence.compareTo(sequence)
        }
    }
}