package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

public enum TaskResult {

    SUCCESS(TaskState.SUCCEEDED),
    SUSPENSION(TaskState.SUSPENDED),
    FILTER(TaskState.FILTERED),
    FAILURE(TaskState.FAILED);

    private static final TaskResult[] VALUES = values();

    private final TaskState state;

    TaskResult(TaskState state) {
        this.state = state;
    }

    public static TaskResult ofOrdinal(int ordinal) {
        return VALUES[ordinal];
    }

    public TaskState toState() {
        return state;
    }

    public TaskResult merge(TaskResult other) {
        return ordinal() > other.ordinal() ? this : other;
    }
}
