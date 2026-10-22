package ru.quipy.common.utils

import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

@Timeout(5)
class SlidingWindowRateLimiterTest {
    @Test
    fun `permits expire individually at the sliding window boundary`() {
        var now = 0L
        val limiter = SlidingWindowRateLimiter(2, Duration.ofNanos(100)) { now }
        assertTrue(limiter.tick())
        now = 20
        assertTrue(limiter.tick())
        now = 99
        assertFalse(limiter.tick())
        now = 100
        assertTrue(limiter.tick())
        assertFalse(limiter.tick())
        now = 119
        assertFalse(limiter.tick())
        now = 120
        assertTrue(limiter.tick())
    }

    @Test
    fun `concurrent callers cannot exceed the window limit`() {
        val limiter = SlidingWindowRateLimiter(3, Duration.ofSeconds(1)) { 0 }
        val executor = Executors.newFixedThreadPool(8)
        try {
            val results = executor.invokeAll(List(100) { Callable { limiter.tick() } })
            assertEquals(3, results.count { it.get(1, TimeUnit.SECONDS) })
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    fun `expired waiting budget does not consume a future permit`() {
        var now = 0L
        val limiter = SlidingWindowRateLimiter(1, Duration.ofNanos(100)) { now }
        assertTrue(limiter.tick())
        assertFalse(limiter.tickBlocking(Duration.ofMillis(20)))
        now = 100
        assertTrue(limiter.tick())
        assertFalse(limiter.tickBlocking(Duration.ZERO))
    }

    @Test
    fun `interrupted waiter preserves the interrupt flag`() {
        val limiter = SlidingWindowRateLimiter(1, Duration.ofSeconds(1))
        Thread.currentThread().interrupt()
        try {
            assertThrows(InterruptedException::class.java) { limiter.tickBlocking(Duration.ofSeconds(1)) }
            assertTrue(Thread.currentThread().isInterrupted)
        } finally {
            Thread.interrupted()
        }
        assertTrue(limiter.tick(), "Interrupted waiting must not consume a permit")
    }
}
