package ru.quipy.common.utils.ratelimiter.implementation

import ru.quipy.common.utils.ratelimiter.RateLimiter
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class PacedRateLimiter(rate: Long, window: Duration) : RateLimiter {
    private val lock = ReentrantLock(true)
    private val intervalNanos: Long
    private var nextPermitAt = System.nanoTime()

    init {
        require(rate > 0)
        val windowNanos = window.toNanos()
        require(windowNanos > 0)
        intervalNanos = windowNanos / rate + if (windowNanos % rate == 0L) 0L else 1L
    }

    override fun tick(): Boolean {
        if (!lock.tryLock(0, TimeUnit.NANOSECONDS)) return false
        try {
            val now = System.nanoTime()
            if (nextPermitAt - now > 0) return false
            nextPermitAt = now + intervalNanos
            return true
        } finally {
            lock.unlock()
        }
    }

    override fun tickBlocking(timeout: Duration): Boolean {
        val timeoutNanos = timeout.toNanos()
        if (timeoutNanos <= 0) return false
        val deadline = System.nanoTime() + timeoutNanos
        if (!lock.tryLock(timeoutNanos, TimeUnit.NANOSECONDS)) return false
        try {
            while (true) {
                val now = System.nanoTime()
                val remaining = deadline - now
                if (remaining <= 0) return false
                val waitNanos = nextPermitAt - now
                if (waitNanos <= 0) {
                    nextPermitAt = now + intervalNanos
                    return true
                }
                TimeUnit.NANOSECONDS.sleep(minOf(waitNanos, remaining))
            }
        } finally {
            lock.unlock()
        }
    }
}
