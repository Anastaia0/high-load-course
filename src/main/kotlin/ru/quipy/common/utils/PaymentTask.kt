package ru.quipy.common.utils

class PaymentTask(
    val amount: Int,
    val latestStart: Long,
    val sequence: Long,
    private val action: () -> Unit
) : Runnable {

    override fun run() = action()

}
