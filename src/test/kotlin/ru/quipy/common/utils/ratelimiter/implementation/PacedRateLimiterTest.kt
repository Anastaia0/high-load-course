package ru.quipy.common.utils.ratelimiter.implementation

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class PacedRateLimiterTest {
    @Test
    fun expiredWaitDoesNotAcquireAPermit() {
        val limiter = PacedRateLimiter(1, Duration.ofSeconds(10))

        assertFalse(limiter.tickBlocking(Duration.ZERO))
        assertTrue(limiter.tick())
        assertFalse(limiter.tickBlocking(Duration.ofMillis(10)))
        assertFalse(limiter.tick())
    }

    @Test
    fun concurrentWaitersArePacedWithoutAnInitialBurst() {
        val limiter = PacedRateLimiter(20, Duration.ofSeconds(1))
        val executor = Executors.newFixedThreadPool(10)
        val start = CountDownLatch(1)
        try {
            val requests = List(10) {
                executor.submit(Callable {
                    start.await()
                    limiter.tickBlocking(Duration.ofSeconds(5))
                })
            }
            val startedAt = System.nanoTime()
            start.countDown()
            requests.forEach { assertTrue(it.get(10, TimeUnit.SECONDS)) }

            val elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt)
            assertTrue(elapsedMs >= 450, "Concurrent requests must be spread over at least 450 ms")
        } finally {
            executor.shutdownNow()
        }
    }
}
