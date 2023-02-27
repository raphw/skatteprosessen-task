package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface TaskSource<TRANSACTION, EXCEPTION extends Exception> {

    default Optional<Task> poll(
        TRANSACTION transaction, String topic
    ) throws EXCEPTION {
        return poll(transaction, topic, Order.FIRST_IN_FIRST_OUT);
    }

    default Optional<Task> poll(
        TRANSACTION transaction, String topic, Order order
    ) throws EXCEPTION {
        return poll(transaction, topic, order, 1).stream().findFirst();
    }

    default Set<Task> poll(
        TRANSACTION transaction, String topic, int size
    ) throws EXCEPTION {
        return poll(transaction, topic, Order.FIRST_IN_FIRST_OUT, size);
    }

    Set<Task> poll(
        TRANSACTION transaction, String topic, Order order, int size
    ) throws EXCEPTION;

    default void complete(
        TRANSACTION transaction, String topic, Task task, TaskDecision decision
    ) throws EXCEPTION {
        complete(transaction, topic, Collections.singletonMap(task, decision));
    }

    void complete(
        TRANSACTION transaction, String topic, Map<Task, TaskDecision> decisions
    ) throws EXCEPTION;

    enum Order {
        FIRST_IN_FIRST_OUT,
        LAST_IN_FIRST_OUT
    }
}
