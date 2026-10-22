package ru.quipy.apigateway

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.springframework.test.util.ReflectionTestUtils
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.*
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.BlockingPaymentService
import ru.quipy.payments.logic.OrderPayer
import ru.quipy.payments.logic.paymentEventStore

@Timeout(10)
class APIControllerTest {
    @Test
    fun `queue overflow returns 429 immediately and leaves the rejected order unchanged`() {
        val service = BlockingPaymentService()
        val payer = OrderPayer(paymentEventStore(), service, SimpleMeterRegistry(), "test", 1)
        val repository = OrderRepository()
        val controller = APIController()
        ReflectionTestUtils.setField(controller, "orderPayer", payer)
        ReflectionTestUtils.setField(controller, "orderRepository", repository)
        val mvc = MockMvcBuilders.standaloneSetup(controller).build()
        fun order() = repository.save(APIController.Order(
            UUID.randomUUID(), UUID.randomUUID(), System.currentTimeMillis(), APIController.OrderStatus.COLLECTING, 100
        ))
        val deadline = System.currentTimeMillis() + 30_000
        try {
            val first = order()
            mvc.perform(post("/orders/" + first.id + "/payment").param("deadline", deadline.toString()))
                .andExpect(status().isOk)
                .andExpect(jsonPath("$.timestamp").isNumber)
                .andExpect(jsonPath("$.transactionId").isString)
            assertNotNull(service.received.poll(2, TimeUnit.SECONDS))
            assertEquals(APIController.OrderStatus.PAYMENT_IN_PROGRESS, repository.findById(first.id)!!.status)
            val queued = order()
            mvc.perform(post("/orders/" + queued.id + "/payment").param("deadline", deadline.toString()))
                .andExpect(status().isOk)
            val rejected = order()
            assertTimeout(Duration.ofMillis(500)) {
                mvc.perform(post("/orders/" + rejected.id + "/payment").param("deadline", deadline.toString()))
                    .andExpect(status().isTooManyRequests)
                    .andExpect(header().string("Retry-After", "1"))
            }
            assertEquals(APIController.OrderStatus.COLLECTING, repository.findById(rejected.id)!!.status)
        } finally {
            service.release.countDown()
            payer.shutdown()
        }
    }
}
