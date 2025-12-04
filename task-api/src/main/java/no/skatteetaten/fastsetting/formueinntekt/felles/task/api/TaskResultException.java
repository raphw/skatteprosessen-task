package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import java.time.Duration;

public class TaskResultException extends RuntimeException {

    private final TaskResult result;
    private final boolean continued;
    private final Duration duration;

    public TaskResultException(TaskResult result) {
        this.result = result;
        continued = result == TaskResult.SUCCESS;
        duration = null;
    }

    public TaskResultException(TaskResult result, boolean continued) {
        this.result = result;
        this.continued = continued;
        duration = null;
    }

    public TaskResultException(String message) {
        super(message);
        result = TaskResult.FAILURE;
        continued = false;
        duration = null;
    }

    public TaskResultException(Throwable cause) {
        super(cause);
        result = TaskResult.FAILURE;
        continued = false;
        duration = null;
    }

    public TaskResultException(TaskResult result, Throwable cause) {
        super(cause);
        this.result = result;
        continued = result == TaskResult.SUCCESS;
        duration = null;
    }

    public TaskResultException(TaskResult result, String message) {
        super(message);
        this.result = result;
        continued = result == TaskResult.SUCCESS;
        duration = null;
    }

    public TaskResultException(TaskResult result, Throwable cause, boolean continued) {
        super(cause);
        this.result = result;
        this.continued = continued;
        duration = null;
    }

    public TaskResultException(TaskResult result, String message, boolean continued) {
        super(message);
        this.result = result;
        this.continued = continued;
        duration = null;
    }

    public TaskResultException(TaskResult result, Duration duration) {
        this.result = result;
        continued = result == TaskResult.SUCCESS;
        this.duration = duration;
    }

    public TaskResultException(TaskResult result, Throwable cause, Duration duration) {
        super(cause);
        this.result = result;
        continued = result == TaskResult.SUCCESS;
        this.duration = duration;
    }

    public TaskResultException(TaskResult result, String message, Duration duration) {
        super(message);
        this.result = result;
        continued = result == TaskResult.SUCCESS;
        this.duration = duration;
    }

    public TaskResultException(TaskResult result, String message, boolean continued, Duration duration) {
        super(message);
        this.result = result;
        this.continued = continued;
        this.duration = duration;
    }

    public TaskResult getResult() {
        return result;
    }

    public TaskDecision toDecision() {
        TaskDecision decision = getCause() == null
            ? new TaskDecision(result, getMessage())
            : new TaskDecision(result, getCause());
        return decision.withContinuation(continued).withScheduling(duration);
    }
}
