package ru.quipy.common.utils.ratelimiter.implementation

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.ratelimiter.RateLimiter
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration.Companion.milliseconds

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlidingWindowRateLimiter::class.java)
    }

    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val permitsInWindow = AtomicLong(0)
    private val acceptedRequests = PriorityBlockingQueue<Measure>(10_000)

    override fun tick(): Boolean {
        while (true) {
            val observedPermits = permitsInWindow.get()
            if (observedPermits >= rate) return false
            if (permitsInWindow.compareAndSet(observedPermits, observedPermits + 1)) {
                acceptedRequests.add(Measure(1, System.currentTimeMillis()))
                return true
            }
        }
    }

    data class Measure(
        val value: Long,
        val timestamp: Long
    ) : Comparable<Measure> {
        override fun compareTo(other: Measure): Int {
            return timestamp.compareTo(other.timestamp)
        }
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            val oldestRequest = acceptedRequests.peek()
            val windowStartMillis = System.currentTimeMillis() - window.toMillis()
            if (oldestRequest == null) {
                delay(1L.milliseconds)
                continue
            }
            if (oldestRequest.timestamp > windowStartMillis) {
                delay((oldestRequest.timestamp - windowStartMillis).milliseconds)
                continue
            }
            permitsInWindow.addAndGet(-1)
            acceptedRequests.take()
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }
}
