package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import org.mockito.Answers
import org.mockito.Mockito
import ru.quipy.core.BasicAggregateRegistry
import ru.quipy.core.EventSourcingProperties
import ru.quipy.core.EventSourcingService
import ru.quipy.core.EventSourcingServiceFactory
import ru.quipy.database.EventStore
import ru.quipy.domain.EventRecord
import ru.quipy.mapper.JsonEventMapper
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.PaymentCreatedEvent
import ru.quipy.payments.api.PaymentProcessedEvent
import ru.quipy.payments.api.PaymentSubmittedEvent

internal fun paymentEventStore(events: MutableList<Any> = CopyOnWriteArrayList()):
    EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState> {
    val aggregates = BasicAggregateRegistry()
    aggregates.register<UUID, PaymentAggregate, PaymentAggregateState>(
        PaymentAggregate::class, PaymentAggregateState::class
    ) {
        registerStateTransition(PaymentCreatedEvent::class, PaymentAggregateState::paymentCreatedApply)
        registerStateTransition(PaymentSubmittedEvent::class, PaymentAggregateState::paymentSubmittedApply)
        registerStateTransition(PaymentProcessedEvent::class, PaymentAggregateState::paymentSubmittedApply)
    }
    val mapper = JsonEventMapper(ObjectMapper().registerKotlinModule().registerModule(JavaTimeModule()))
    val records = CopyOnWriteArrayList<EventRecord>()
    val store = Mockito.mock(EventStore::class.java) { invocation ->
        when (invocation.method.name) {
            "insertEventRecord" -> {
                val record = invocation.arguments[1] as EventRecord
                records.add(record)
                events.add(mapper.toEvent(
                    record.payload,
                    aggregates.getEventInfo(PaymentAggregate::class)!!.getEventTypeByName(record.eventTitle)
                ))
                null
            }
            "findEventRecordsWithAggregateVersionGraterThan" -> records.filter {
                it.aggregateId == invocation.arguments[1] &&
                    it.aggregateVersion > invocation.arguments[2] as Long
            }
            "tableExists" -> true
            else -> Answers.RETURNS_DEFAULTS.answer(invocation)
        }
    }
    return EventSourcingServiceFactory(
        aggregates, mapper, store, EventSourcingProperties(sagasEnabled = false, snapshotsEnabled = false)
    ).create()
}
