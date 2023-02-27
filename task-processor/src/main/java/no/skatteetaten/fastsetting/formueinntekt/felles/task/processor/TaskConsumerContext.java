package no.skatteetaten.fastsetting.formueinntekt.felles.task.processor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskConsumer;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskCreation;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskDecision;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSink;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskSupplement;

public class TaskConsumerContext<TRANSACTION, EXCEPTION extends Exception, SUPPLEMENT extends TaskSupplement> implements TaskConsumer, TaskContext<TRANSACTION, EXCEPTION, SUPPLEMENT> {

    private final Set<Task> tasks;

    private final TaskSink<TRANSACTION, EXCEPTION> taskSink;

    private final Function<String, Optional<TaskProcessor>> processors;

    private final BiFunction<Task, TaskCreation, Stream<TaskCreation>> postprocessor;

    private final Map<String, List<Definition>> topics = new ConcurrentHashMap<>();

    public TaskConsumerContext(
        Set<Task> tasks,
        TaskSink<TRANSACTION, EXCEPTION> taskSink,
        Function<String, Optional<TaskProcessor>> processors
    ) {
        this.tasks = tasks;
        this.taskSink = taskSink;
        this.processors = processors;
        postprocessor = (task, creation) -> Stream.of(creation);
    }

    public TaskConsumerContext(
        Set<Task> tasks,
        TaskSink<TRANSACTION, EXCEPTION> taskSink,
        Function<String, Optional<TaskProcessor>> processors,
        BiFunction<Task, TaskCreation, Stream<TaskCreation>> postprocessor
    ) {
        this.tasks = tasks;
        this.taskSink = taskSink;
        this.processors = processors;
        this.postprocessor = postprocessor;
    }

    @Override
    public void pushByTask(
        Set<Task> tasks,
        String topic,
        TaskSink.Insertion insertion,
        Function<Task, Collection<TaskCreation>> resolver
    ) {
        if (!this.tasks.containsAll(tasks)) {
            throw new IllegalArgumentException("Cannot write tasks for unknown tasks: " + tasks.stream()
                .filter(task -> !this.tasks.contains(task))
                .collect(Collectors.toList()));
        }
        this.topics.computeIfAbsent(topic, ignored -> new CopyOnWriteArrayList<>()).add(new Definition(tasks, insertion, resolver));
    }

    @Override
    public CompletionStage<TaskContextCompletion<TRANSACTION, EXCEPTION>> apply(
        Map<Task, TaskDecision> decisions,
        Executor executor,
        SUPPLEMENT supplement
    ) {
        CompletableFuture<TaskContextCompletion<TRANSACTION, EXCEPTION>> future = new CompletableFuture<>();
        if (topics.isEmpty()) {
            future.complete(transaction -> decisions);
        } else {
            executor.execute(() -> {
                try {
                    Set<Task> succeeded = decisions.entrySet().stream()
                        .filter(entry -> entry.getValue().isContinued())
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toSet());
                    Map<String, List<Resolved>> resolved = topics.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().stream()
                            .flatMap(definition -> definition.resolve(succeeded, postprocessor).stream())
                            .collect(Collectors.toList())
                    ));
                    if (resolved.values().stream().mapToLong(Collection::size).sum() == 0) {
                        future.complete(transaction -> decisions);
                    } else {
                        supplement.register(TopicsToResume.class, new TopicsToResume(resolved.keySet()));
                        future.complete(transaction -> {
                            for (Map.Entry<String, List<Resolved>> entry : resolved.entrySet()) {
                                Iterator<Resolved> it = entry.getValue().iterator();
                                Resolved current = it.next();
                                TaskSink.Insertion insertion = current.getInsertion();
                                List<TaskCreation> creations = new ArrayList<>(resolved.size());
                                creations.add(current.getCreation());
                                while (it.hasNext()) {
                                    current = it.next();
                                    if (current.getInsertion() != insertion) {
                                        taskSink.push(transaction, entry.getKey(), insertion, creations);
                                        insertion = current.getInsertion();
                                        creations.clear();
                                    }
                                    creations.add(current.getCreation());
                                }
                                taskSink.push(transaction, entry.getKey(), insertion, creations);
                            }
                            return decisions;
                        });
                    }
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        }
        return future;
    }

    @Override
    public void onAfterTransaction(Executor executor, SUPPLEMENT supplement) {
        supplement.probe(TopicsToResume.class).ifPresent(resume -> executor.execute(
            () -> resume.getTopics().forEach(topic -> processors.apply(topic).ifPresent(TaskProcessor::resume))
        ));
    }

    static class Definition {

        private final Set<Task> tasks;
        private final TaskSink.Insertion insertion;
        private final Function<Task, Collection<TaskCreation>> resolver;

        Definition(
            Set<Task> tasks,
            TaskSink.Insertion insertion,
            Function<Task, Collection<TaskCreation>> resolver
        ) {
            this.tasks = tasks;
            this.insertion = insertion;
            this.resolver = resolver;
        }

        List<Resolved> resolve(Set<Task> succeeded, BiFunction<Task, TaskCreation, Stream<TaskCreation>> postprocessor) {
            return tasks.stream().filter(succeeded::contains).flatMap(task -> resolver.apply(task).stream()
                .flatMap(creation -> postprocessor.apply(task, creation).map(transformed -> new Resolved(task, insertion, transformed))))
                .collect(Collectors.toList());
        }
    }

    static class Resolved {

        private final Task task;
        private final TaskSink.Insertion insertion;
        private final TaskCreation creation;

        Resolved(Task task, TaskSink.Insertion insertion, TaskCreation creation) {
            this.task = task;
            this.creation = creation;
            this.insertion = insertion;
        }

        Task getTask() {
            return task;
        }

        TaskSink.Insertion getInsertion() {
            return insertion;
        }

        TaskCreation getCreation() {
            return creation;
        }
    }

    static class TopicsToResume {

        private final Set<String> topics;

        TopicsToResume(Set<String> topics) {
            this.topics = topics;
        }

        Set<String> getTopics() {
            return topics;
        }
    }
}
