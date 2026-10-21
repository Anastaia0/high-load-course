package ru.quipy.common.utils

class PaymentTask(
    private val amount: Int,
    private val deadline: Long,
    private val sequence: Long,
    private val action: () -> Unit
) : Runnable, Comparable<PaymentTask> {

    override fun run() = action()

    override fun compareTo(other: PaymentTask): Int {
        // Сначала задача, у которой раньше заканчивается срок оплаты.
        val deadlineComparison = deadline.compareTo(other.deadline)

        if (deadlineComparison != 0) {
            return deadlineComparison
        }

        // При одинаковом дедлайне сначала более дорогая оплата.
        val amountComparison = other.amount.compareTo(amount)

        if (amountComparison != 0) {
            return amountComparison
        }

        // При остальных равных сначала раньше принятая задача.
        return sequence.compareTo(other.sequence)
    }
}