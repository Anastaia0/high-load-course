package ru.quipy.common.utils

import java.time.Duration
import java.util.ArrayDeque
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class SlidingWindowRateLimiter(
    private val rate: Long,
    window: Duration,
    private val nanoTime: () -> Long = System::nanoTime,
) : RateLimiter {
    private val windowNanos = window.toNanos()
    private val lock = ReentrantLock()
    private val starts = ArrayDeque<Long>()

    init {
        require(rate > 0) { "Rate limit must be positive" }
        require(windowNanos > 0) { "Rate limit window must be positive" }
    }

    override fun tick(): Boolean = lock.withLock {
        val now = nanoTime()
        while (starts.isNotEmpty() && now - starts.first >= windowNanos) starts.removeFirst()
        if (starts.size.toLong() >= rate) return false
        starts.addLast(now)
        true
    }

    fun tickBlocking() {
        while (!tick()) Thread.sleep(10)
    }

    fun tickBlocking(timeout: Duration): Boolean {
        if (timeout.isNegative || timeout.isZero) return false
        val startedAt = System.nanoTime()
        val budget = timeout.toNanos()
        while (true) {
            if (Thread.currentThread().isInterrupted) throw InterruptedException()
            val remaining = budget - (System.nanoTime() - startedAt)
            if (remaining <= 0) return false
            if (tick()) return true
            TimeUnit.NANOSECONDS.sleep(minOf(TimeUnit.MILLISECONDS.toNanos(10), remaining))
        }
    }
}
