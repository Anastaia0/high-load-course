package ru.quipy.common.utils

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout

@Timeout(5)
class PaymentPriorityQueueTest {
    private fun task(amount: Int, latestStart: Long, sequence: Long) =
        PaymentTask(amount, latestStart, sequence) {}

    @Test
    fun `expensive eligible payments come first and newer requests break ties`() {
        val queue = PaymentPriorityQueue(3) { 100 }
        val cheap = task(10, 1000, 3)
        val expensive = task(100, 200, 1)
        val newer = task(100, 300, 2)
        assertTrue(queue.offer(cheap))
        queue.put(expensive)
        assertTrue(queue.offer(newer, 1, TimeUnit.SECONDS))
        assertSame(newer, queue.take())
        assertSame(expensive, queue.poll())
        assertSame(cheap, queue.poll(1, TimeUnit.SECONDS))
    }

    @Test
    fun `eligibility is reevaluated and expires at the exact latest start`() {
        var now = 99L
        val queue = PaymentPriorityQueue(3) { now }
        val expensive = task(1000, 100, 1)
        val fresh = task(1, 200, 2)
        queue.offer(expensive)
        queue.offer(fresh)
        assertSame(expensive, queue.peek())
        now = 100
        assertSame(fresh, queue.take())
        assertSame(expensive, queue.take(), "Expired tasks must remain available for recording a failure")
        assertNull(queue.poll())
    }

    @Test
    fun `full queue refuses new tasks without evicting accepted work`() {
        val queue = PaymentPriorityQueue(1) { 0 }
        val first = task(1, 100, 1)
        queue.put(first)
        assertEquals(0, queue.remainingCapacity())
        assertFalse(queue.offer(task(1000, 100, 2)))
        assertFalse(queue.offer(task(1000, 100, 2), 1, TimeUnit.MILLISECONDS))
        assertSame(first, queue.poll())
        assertEquals(1, queue.remainingCapacity())
        assertNull(queue.poll(1, TimeUnit.MILLISECONDS))
    }

    @Test
    fun `blocked producer resumes when a task is removed`() {
        val queue = PaymentPriorityQueue(1) { 0 }
        val first = task(1, 100, 1)
        val second = task(2, 100, 2)
        val executor = Executors.newSingleThreadExecutor()
        val started = CountDownLatch(1)
        queue.put(first)
        try {
            val producer = executor.submit {
                started.countDown()
                queue.put(second)
            }
            assertTrue(started.await(1, TimeUnit.SECONDS))
            assertFalse(producer.isDone)
            assertTrue(queue.remove(first))
            producer.get(1, TimeUnit.SECONDS)
            assertSame(second, queue.take())
        } finally {
            executor.shutdownNow()
        }
    }

    @Test
    fun `iterator removal and drain preserve capacity and priority`() {
        val queue = PaymentPriorityQueue(3) { 0 }
        val first = task(1, 100, 1)
        val expensive = task(10, 100, 2)
        val last = task(2, 100, 3)
        queue.addAll(listOf(first, expensive, last))
        val iterator = queue.iterator()
        assertSame(first, iterator.next())
        iterator.remove()
        assertEquals(1, queue.remainingCapacity())
        val drained = mutableListOf<Runnable>()
        assertEquals(1, queue.drainTo(drained, 1))
        assertSame(expensive, drained.single())
        assertSame(last, queue.take())
        assertEquals(3, queue.remainingCapacity())
    }
}
