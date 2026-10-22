package ru.quipy.common.utils

import java.util.AbstractQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class PaymentPriorityQueue(
    private val capacity: Int,
    private val clock: () -> Long = System::currentTimeMillis,
) : AbstractQueue<Runnable>(), BlockingQueue<Runnable> {
    private val lock = ReentrantLock()
    private val notEmpty = lock.newCondition()
    private val notFull = lock.newCondition()
    private val tasks = ArrayList<PaymentTask>()

    init {
        require(capacity > 0) { "Payment queue capacity must be positive" }
    }

    override val size: Int get() = lock.withLock { tasks.size }

    override fun remainingCapacity(): Int = lock.withLock { capacity - tasks.size }

    // Eligibility changes while tasks wait, so a static heap comparator is not sufficient.
    private fun bestIndex(): Int {
        val now = clock()
        var best = -1
        for (index in tasks.indices) {
            if (best == -1 || precedes(tasks[index], tasks[best], now)) best = index
        }
        return best
    }

    private fun precedes(candidate: PaymentTask, current: PaymentTask, now: Long): Boolean {
        val eligible = now < candidate.latestStart
        val currentEligible = now < current.latestStart
        if (eligible != currentEligible) return eligible
        if (eligible && candidate.amount != current.amount) return candidate.amount > current.amount
        return candidate.sequence > current.sequence
    }

    private fun insert(element: Runnable) {
        require(element is PaymentTask) { "Only payment tasks can enter this queue" }
        tasks.add(element)
        notEmpty.signal()
    }

    private fun extract(): Runnable? {
        val index = bestIndex()
        if (index == -1) return null
        return tasks.removeAt(index).also { notFull.signal() }
    }

    override fun offer(element: Runnable): Boolean = lock.withLock {
        if (tasks.size == capacity) return false
        insert(element)
        true
    }

    override fun put(element: Runnable) {
        lock.lockInterruptibly()
        try {
            while (tasks.size == capacity) notFull.await()
            insert(element)
        } finally {
            lock.unlock()
        }
    }

    override fun offer(element: Runnable, timeout: Long, unit: TimeUnit): Boolean {
        var remaining = unit.toNanos(timeout)
        lock.lockInterruptibly()
        try {
            while (tasks.size == capacity) {
                if (remaining <= 0) return false
                remaining = notFull.awaitNanos(remaining)
            }
            insert(element)
            return true
        } finally {
            lock.unlock()
        }
    }

    override fun take(): Runnable {
        lock.lockInterruptibly()
        try {
            while (tasks.isEmpty()) notEmpty.await()
            return extract()!!
        } finally {
            lock.unlock()
        }
    }

    override fun poll(timeout: Long, unit: TimeUnit): Runnable? {
        var remaining = unit.toNanos(timeout)
        lock.lockInterruptibly()
        try {
            while (tasks.isEmpty()) {
                if (remaining <= 0) return null
                remaining = notEmpty.awaitNanos(remaining)
            }
            return extract()
        } finally {
            lock.unlock()
        }
    }

    override fun poll(): Runnable? = lock.withLock { extract() }

    override fun peek(): Runnable? = lock.withLock { tasks.getOrNull(bestIndex()) }

    override fun remove(element: Runnable): Boolean = lock.withLock {
        val index = tasks.indexOfFirst { it === element }
        if (index == -1) return false
        tasks.removeAt(index)
        notFull.signal()
        true
    }

    override fun iterator(): MutableIterator<Runnable> = lock.withLock {
        val snapshot = tasks.toList().iterator()
        object : MutableIterator<Runnable> {
            private var current: Runnable? = null
            override fun hasNext() = snapshot.hasNext()
            override fun next(): Runnable = snapshot.next().also { current = it }
            override fun remove() {
                this@PaymentPriorityQueue.remove(current ?: throw IllegalStateException())
                current = null
            }
        }
    }

    override fun drainTo(target: MutableCollection<in Runnable>): Int = drainTo(target, Int.MAX_VALUE)

    override fun drainTo(target: MutableCollection<in Runnable>, maxElements: Int): Int = lock.withLock {
        require(target !== this)
        var count = 0
        while (count < maxElements && tasks.isNotEmpty()) {
            val index = bestIndex()
            target.add(tasks[index])
            tasks.removeAt(index)
            notFull.signal()
            count++
        }
        count
    }
}
