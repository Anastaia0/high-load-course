package ru.quipy.common.utils

class PaymentTask(
    private val amount: Int,
    private val deadline: Long,
    private val sequence: Long,
    private val action: () -> Unit
) : Runnable, Comparable<PaymentTask> {

    override fun run() = action()

    override fun compareTo(other: PaymentTask): Int {
        val deadlineComparison = deadline.compareTo(other.deadline)

        if (deadlineComparison != 0) {
            return deadlineComparison
        }

        val amountComparison = other.amount.compareTo(amount)

        if (amountComparison != 0) {
            return amountComparison
        }

        return sequence.compareTo(other.sequence)
    }
}
