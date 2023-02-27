package no.skatteetaten.fastsetting.formueinntekt.felles.task.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TaskStateTest {

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {TaskState.ACTIVE, 0, null},
            {TaskState.READY, 1, null},
            {TaskState.EXPIRED, 2, null},
            {TaskState.SUCCEEDED, 3, TaskResult.SUCCESS},
            {TaskState.SUSPENDED, 4, TaskResult.SUSPENSION},
            {TaskState.FILTERED, 5, TaskResult.FILTER},
            {TaskState.FAILED, 6, TaskResult.FAILURE},
            {TaskState.RECREATED, 7, null},
            {TaskState.REDUNDANT, 8, null}
        });
    }

    private final TaskState state;

    private final int ordinal;

    private final TaskResult result;

    public TaskStateTest(TaskState state, int ordinal, TaskResult result) {
        this.state = state;
        this.ordinal = ordinal;
        this.result = result;
    }

    @Test
    public void defines_expected_ordinal() {
        assertThat(state.ordinal()).isEqualTo(ordinal);
    }

    @Test
    public void resolves_ordinal() {
        assertThat(TaskState.ofOrdinal(ordinal)).isEqualTo(state);
    }

    @Test
    public void resolve_result() {
        if (result == null) {
            return;
        }
        assertThat(result.toState()).isEqualTo(state);
        assertThat(TaskResult.ofOrdinal(ordinal - TaskState.SUCCEEDED.ordinal())).isEqualTo(result);
    }
}
