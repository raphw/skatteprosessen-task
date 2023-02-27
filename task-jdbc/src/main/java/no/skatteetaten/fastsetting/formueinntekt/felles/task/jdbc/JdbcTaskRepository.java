package no.skatteetaten.fastsetting.formueinntekt.felles.task.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.Task;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskCreation;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskDecision;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskInfo;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskRepository;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskResult;
import no.skatteetaten.fastsetting.formueinntekt.felles.task.api.TaskState;

public class JdbcTaskRepository implements TaskRepository<Connection, SQLException> {

    public static final String CHANGE_LOG = "liquibase/taskChangeLog.xml";

    protected final boolean concurrent;

    protected final String owner;

    public JdbcTaskRepository(boolean concurrent, String owner) {
        this.concurrent = concurrent;
        this.owner = owner;
    }

    public boolean isConcurrent() {
        return concurrent;
    }

    public String getOwner() {
        return owner;
    }

    @Override
    public boolean register(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
            "MERGE INTO TASK_OWNER "
                + "USING (VALUES ?) AS TARGET (OWNER) "
                + "ON TASK_OWNER.OWNER = TARGET.OWNER "
                + "WHEN NOT MATCHED THEN INSERT (OWNER) VALUES (?)"
        )) {
            ps.setString(1, owner);
            ps.setString(2, owner);
            return ps.executeUpdate() == 1;
        }
    }

    @Override
    public Set<String> owners(Connection conn, long timeout, TimeUnit unit) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT OWNER FROM TASK_OWNER WHERE OWNER != ? AND HEARTBEAT > CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) - INTERVAL '" + unit.toSeconds(timeout) + "' SECOND"
        )) {
            ps.setString(1, owner);
            Set<String> owners = new HashSet<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    owners.add(rs.getString("OWNER"));
                }
            }
            return owners;
        }
    }

    @Override
    public void heartbeat(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
            "UPDATE TASK_OWNER SET HEARTBEAT = CURRENT_TIMESTAMP AT TIME ZONE 'UTC' WHERE OWNER = ?"
        )) {
            ps.setString(1, owner);
            if (ps.executeUpdate() == 0) {
                throw new IllegalStateException("Could not send heartbeat for " + owner);
            }
        }
    }

    @Override
    public void expire(Connection conn, long timeout, TimeUnit unit) throws SQLException {
        if (timeout < 0) {
            throw new IllegalArgumentException("Cannot accept negative timeout");
        }
        try (PreparedStatement ps = conn.prepareStatement(
            "UPDATE TASK SET STATE = ? "
                + (concurrent ? "WHERE (TOPIC, SEQUENCE) IN (SELECT TOPIC, SEQUENCE FROM TASK " : "")
                + "WHERE STATE = ? "
                + "AND OWNER IN ("
                + "SELECT OWNER "
                + "FROM TASK_OWNER "
                + "WHERE HEARTBEAT < CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) - INTERVAL '" + unit.toSeconds(timeout) + "' SECOND "
                + "AND OWNER != ?"
                + ")"
                + (concurrent ? " FOR UPDATE SKIP LOCKED)" : "")
        )) {
            ps.setInt(1, TaskState.EXPIRED.ordinal());
            ps.setInt(2, TaskState.ACTIVE.ordinal());
            ps.setString(3, owner);
            ps.executeUpdate();
        }
    }

    @Override
    public boolean initialize(Connection conn, String topic) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
            "MERGE INTO TASK_TOPIC "
                + "USING (VALUES ?) AS TARGET (TOPIC) "
                + "ON TASK_TOPIC.TOPIC = TARGET.TOPIC "
                + "WHEN NOT MATCHED THEN INSERT (TOPIC) VALUES (?)"
        )) {
            ps.setString(1, topic);
            ps.setString(2, topic);
            return ps.executeUpdate() == 1;
        }
    }

    @Override
    public List<Task> push(
        Connection conn,
        String topic,
        Insertion insertion,
        Collection<TaskCreation> creations
    ) throws SQLException {
        if (creations.isEmpty()) {
            return Collections.emptyList();
        }
        List<Task> tasks = new ArrayList<>(creations.size());
        Map<String, Long> duplicates = insertion == Insertion.REPLACE
            ? creations.stream().collect(Collectors.groupingBy(TaskCreation::getIdentifier, Collectors.counting()))
            : null;
        try (PreparedStatement ps = conn.prepareStatement(
            insertion == Insertion.REPLACE
                ? "INSERT INTO TASK (TOPIC, IDENTIFIER, INPUT, REFERENCE, STATE) VALUES (?, ?, ?, ?, ?)"
                : "INSERT INTO TASK (TOPIC, IDENTIFIER, INPUT, REFERENCE) VALUES (?, ?, ?, ?)",
            new String[] {"SEQUENCE"}
        )) {
            for (TaskCreation creation : creations) {
                ps.setString(1, topic);
                ps.setString(2, creation.getIdentifier());
                ps.setString(3, creation.getInput().orElse(null));
                ps.setString(4, creation.getReference().orElse(null));
                if (duplicates != null) {
                    ps.setInt(5, (duplicates.compute(creation.getIdentifier(), (ignored, value) -> --value).intValue() == 0
                        ? TaskState.READY
                        : TaskState.REDUNDANT).ordinal());
                }
                ps.addBatch();
            }
            try {
                if (IntStream.of(ps.executeBatch()).sum() != creations.size()) {
                    throw new IllegalStateException("Could not insert expected amount of tasks for " + topic);
                }
            } catch (SQLException e) {
                if (e.getMessage() != null && e.getMessage().contains("TASK_TOPIC_REF")) {
                    throw new IllegalArgumentException("Topic " + topic + " is not registered - cannot create tasks without handler", e);
                }
                throw e;
            }
            try (ResultSet rs = ps.getGeneratedKeys()) {
                for (TaskCreation creation : creations) {
                    if (!rs.next()) {
                        throw new IllegalStateException("Expected generated key for task");
                    }
                    tasks.add(new Task(
                        rs.getLong(1),
                        creation.getIdentifier(),
                        creation.getInput().orElse(null)
                    ));
                }
                if (rs.next()) {
                    throw new IllegalStateException("Unexpected generated key");
                }
            }
        }
        if (insertion == Insertion.REPLACE) {
            doFilter(conn, topic, creations.stream().map(TaskCreation::getIdentifier).collect(Collectors.toSet()), tasks.get(0).getSequence());
        }
        return tasks;
    }

    void doFilter(Connection conn, String topic, Collection<String> identifiers, long sequence) throws SQLException {
        Set<String> checked = new HashSet<>();
        try (PreparedStatement ps = conn.prepareStatement(
            "UPDATE TASK "
                + "SET STATE = ?, DESCENT = GREATEST(STATE, DESCENT) "
                + "WHERE TOPIC = ? "
                + (concurrent ? "AND SEQUENCE IN (SELECT SEQUENCE FROM TASK WHERE TOPIC = ? " : "")
                + "AND SEQUENCE < ? "
                + "AND IDENTIFIER = ? "
                + "AND STATE BETWEEN ? AND ?"
                + (concurrent ? " FOR UPDATE SKIP LOCKED)" : "")
        )) {
            for (String identifier : identifiers) {
                if (checked.add(identifier)) {
                    ps.setInt(1, TaskState.REDUNDANT.ordinal());
                    ps.setString(2, topic);
                    if (concurrent) {
                        ps.setString(3, topic);
                    }
                    ps.setLong(3 + (concurrent ? 1 : 0), sequence);
                    ps.setString(4 + (concurrent ? 1 : 0), identifier);
                    ps.setInt(5 + (concurrent ? 1 : 0), TaskState.READY.ordinal());
                    ps.setInt(6 + (concurrent ? 1 : 0), TaskState.FAILED.ordinal());
                    ps.addBatch();
                }
            }
            ps.executeBatch();
        }
    }

    @Override
    public Set<Task> poll(
        Connection conn, String topic, Order order, int size
    ) throws SQLException {
        Set<Task> tasks = doPoll(conn, topic, TaskState.READY, order, INCEPTION, Long.MAX_VALUE, size);
        if (tasks.isEmpty()) {
            return Collections.emptySet();
        }
        try (PreparedStatement ps = conn.prepareStatement(
            "UPDATE TASK "
                + "SET STATE = ?, OWNER = ? "
                + "WHERE TOPIC = ? "
                + "AND STATE = ? "
                + "AND SEQUENCE BETWEEN ? AND ?"
        )) {
            Iterator<Task> it = tasks.stream().sorted().iterator();
            Task task = it.next();
            long from = task.getSequence(), to = from;
            do {
                task = it.hasNext() ? it.next() : null;
                if (task != null && to == task.getSequence() - 1) {
                    to = task.getSequence();
                } else {
                    ps.setInt(1, TaskState.ACTIVE.ordinal());
                    ps.setString(2, owner);
                    ps.setString(3, topic);
                    ps.setInt(4, TaskState.READY.ordinal());
                    ps.setLong(5, from);
                    ps.setLong(6, to);
                    ps.addBatch();
                    if (task != null) {
                        from = task.getSequence();
                        to = from;
                    }
                }
            } while (task != null);
            if (IntStream.of(ps.executeBatch()).sum() != tasks.size()) {
                throw new IllegalStateException("Unable to lock expected amount of tasks for " + topic);
            }
        }
        return tasks;
    }

    Set<Task> doPoll(
        Connection conn, String topic, TaskState state, Order order, long from, long to, int size
    ) throws SQLException {
        if (size == 0) {
            return Collections.emptySet();
        }
        Set<Task> tasks = new LinkedHashSet<>();
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT SEQUENCE, IDENTIFIER, INPUT "
                + "FROM TASK "
                + "WHERE TOPIC = ? "
                + "AND STATE = ? "
                + (from == 0 && to == Long.MAX_VALUE ? "" : "AND SEQUENCE BETWEEN ? AND ? ")
                + "ORDER BY SEQUENCE " + (order == Order.LAST_IN_FIRST_OUT ? "DESC " : "ASC ")
                + (concurrent ? "FOR UPDATE SKIP LOCKED " : "")
                + "FETCH FIRST ? ROWS ONLY"
        )) {
            ps.setString(1, topic);
            ps.setInt(2, state.ordinal());
            if (from == 0 && to == Long.MAX_VALUE) {
                ps.setInt(3, size);
            } else {
                ps.setLong(3, from);
                ps.setLong(4, to);
                ps.setInt(5, size);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    tasks.add(new Task(
                        rs.getLong("SEQUENCE"),
                        rs.getString("IDENTIFIER"),
                        rs.getString("INPUT")
                    ));
                }
            }
        }
        return tasks;
    }

    void doTranscribe(
        Connection conn,
        String topic,
        Collection<? extends Task> tasks,
        Function<Task, TaskState> resolver,
        TaskState lower,
        TaskState upper
    ) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
            "UPDATE TASK "
                + "SET STATE = ?, DESCENT = GREATEST(STATE, DESCENT) "
                + "WHERE TOPIC = ? "
                + "AND STATE BETWEEN ? AND ? "
                + "AND SEQUENCE BETWEEN ? AND ?"
        )) {
            Iterator<? extends Task> it = tasks.stream().sorted().iterator();
            Task task = it.next();
            long from = task.getSequence(), to = from;
            TaskState state = resolver.apply(task);
            do {
                task = it.hasNext() ? it.next() : null;
                if (task != null && resolver.apply(task) == state && to == task.getSequence() - 1) {
                    to = to + 1;
                } else {
                    ps.setInt(1, state.ordinal());
                    ps.setString(2, topic);
                    ps.setInt(3, lower.ordinal());
                    ps.setInt(4, upper.ordinal());
                    ps.setLong(5, from);
                    ps.setLong(6, to);
                    ps.addBatch();
                    if (task != null) {
                        from = task.getSequence();
                        to = from;
                        state = resolver.apply(task);
                    }
                }
            } while (task != null);
            if (IntStream.of(ps.executeBatch()).sum() != tasks.size()) {
                throw new IllegalStateException("Unable to transcribe expected amount of tasks for " + topic);
            }
        }
    }

    void doComplete(Connection conn, String topic, Map<Task, TaskDecision> decisions) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
            "UPDATE TASK "
                + "SET STATE = ?, DESCENT = GREATEST(STATE, DESCENT), OUTPUT = ?, COMPLETED = CURRENT_TIMESTAMP AT TIME ZONE 'UTC' "
                + "WHERE TOPIC = ? "
                + "AND SEQUENCE BETWEEN ? AND ? "
                + "AND STATE = ? "
                + "AND OWNER = ?"
        )) {
            Iterator<Map.Entry<Task, TaskDecision>> it = decisions.entrySet().stream().sorted(Map.Entry.comparingByKey()).iterator();
            Map.Entry<Task, TaskDecision> entry = it.next();
            long from = entry.getKey().getSequence(), to = from;
            TaskDecision decision = entry.getValue();
            do {
                entry = it.hasNext() ? it.next() : null;
                if (entry != null && entry.getValue().isSameAs(decision) && to == entry.getKey().getSequence() - 1) {
                    to = to + 1;
                } else {
                    ps.setInt(1, decision.getResult().toState().ordinal());
                    ps.setString(2, decision.toMessage().orElse(null));
                    ps.setString(3, topic);
                    ps.setLong(4, from);
                    ps.setLong(5, to);
                    ps.setInt(6, TaskState.ACTIVE.ordinal());
                    ps.setString(7, owner);
                    ps.addBatch();
                    if (entry != null) {
                        from = entry.getKey().getSequence();
                        to = from;
                        decision = entry.getValue();
                    }
                }
            } while (entry != null);
            if (IntStream.of(ps.executeBatch()).sum() != decisions.size()) {
                throw new IllegalStateException("Unable to transcribe expected amount of tasks for " + topic);
            }
        }
    }

    <TASK extends Task> Map<TASK, Task> doRecreate(
        Connection conn, String topic, Insertion insertion, Collection<TASK> tasks, TaskState lower, TaskState upper
    ) throws SQLException {
        doTranscribe(conn, topic, tasks, task -> TaskState.RECREATED, lower, upper);
        List<Task> recreated = push(conn, topic, insertion, tasks.stream()
            .map(task -> new TaskCreation(task.getIdentifier(), task.getInput().orElse(null)))
            .collect(Collectors.toList()));
        Iterator<TASK> it = tasks.iterator();
        return recreated.stream().collect(LinkedHashMap::new, (map, task) -> map.put(it.next(), task), Map::putAll);
    }

    <TASK extends Task> Map<TASK, Task> doReset(
        Connection conn, String topic, Collection<TASK> tasks, TaskState lower, TaskState upper
    ) throws SQLException {
        doTranscribe(conn, topic, tasks, task -> TaskState.READY, lower, upper);
        return tasks.stream().collect(Collectors.toMap(Function.identity(), Function.identity()));
    }

    @Override
    public void complete(
        Connection conn, String topic, Map<Task, TaskDecision> decisions
    ) throws SQLException {
        if (decisions.isEmpty()) {
            return;
        }
        doComplete(conn, topic, decisions);
    }

    @Override
    public void reassign(
        Connection conn, String topic, Map<Task, TaskResult> tasks
    ) throws SQLException {
        if (tasks.isEmpty()) {
            return;
        }
        doTranscribe(conn, topic, tasks.keySet(), task -> tasks.get(task).toState(), TaskState.EXPIRED, TaskState.FAILED);
    }

    @Override
    public long reassignAll(
        Connection conn, String topic, Revived revived, TaskResult result, long from, long to
    ) throws SQLException {
        if (revived.toState() == result.toState() || from > to) {
            return 0;
        }
        try (PreparedStatement ps = conn.prepareStatement(
            "UPDATE TASK "
                + "SET STATE = ?, DESCENT = GREATEST(STATE, DESCENT) "
                + "WHERE TOPIC = ? "
                + "AND STATE = ? "
                + (from == INCEPTION && to == Long.MAX_VALUE ? "" : " AND SEQUENCE BETWEEN ? AND ?")
        )) {
            ps.setInt(1, result.toState().ordinal());
            ps.setString(2, topic);
            ps.setInt(3, revived.toState().ordinal());
            if (from != INCEPTION || to != Long.MAX_VALUE) {
                ps.setLong(4, from);
                ps.setLong(5, to);
            }
            return ps.executeLargeUpdate();
        }
    }

    @Override
    public Map<Task, Task> recreate(
        Connection conn, String topic, Revivification revivification, Revived revived, long from, long to, int size
    ) throws SQLException {
        Set<Task> tasks = doPoll(conn, topic, revived.toState(), Order.FIRST_IN_FIRST_OUT, from, to, size);
        if (tasks.isEmpty()) {
            return Collections.emptyMap();
        }
        switch (revivification) {
        case APPEND:
            return doRecreate(conn, topic, Insertion.APPEND, tasks, revived.toState(), revived.toState());
        case REPLACE:
            return doRecreate(conn, topic, Insertion.REPLACE, tasks, revived.toState(), revived.toState());
        case RESET:
            return doReset(conn, topic, tasks, revived.toState(), revived.toState());
        default:
            throw new IllegalStateException();
        }
    }

    @Override
    public <TASK extends Task> Map<TASK, Task> recreate(
        Connection conn, String topic, Revivification revivification, Set<TASK> tasks
    ) throws SQLException {
        if (tasks.isEmpty()) {
            return Collections.emptyMap();
        }
        switch (revivification) {
        case APPEND:
            return doRecreate(conn, topic, Insertion.APPEND, tasks, TaskState.EXPIRED, TaskState.FAILED);
        case REPLACE:
            return doRecreate(conn, topic, Insertion.REPLACE, tasks, TaskState.EXPIRED, TaskState.FAILED);
        case RESET:
            return doReset(conn, topic, tasks, TaskState.EXPIRED, TaskState.FAILED);
        default:
            throw new IllegalStateException();
        }
    }

    @Override
    public List<TaskInfo> page(
        Connection conn, String topic, long sequence, int size, Direction direction
    ) throws SQLException {
        if (size < 1 || direction.isOutOfBound(sequence)) {
            return Collections.emptyList();
        }
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT SEQUENCE, IDENTIFIER, OWNER, REFERENCE, STATE, DESCENT, CREATED, COMPLETED, INPUT, OUTPUT "
                + "FROM TASK "
                + "WHERE TOPIC = ? "
                + (direction.isLimit(sequence) ? "" : ("AND SEQUENCE " + (direction == Direction.BACKWARD ? "<" : ">") + " ? "))
                + "ORDER BY SEQUENCE " + (direction == Direction.FORWARD ? "" : "DESC ")
                + "FETCH FIRST ? ROWS ONLY"
        )) {
            ps.setString(1, topic);
            if (direction.isLimit(sequence)) {
                ps.setInt(2, size);
            } else {
                ps.setLong(2, sequence);
                ps.setInt(3, size);
            }
            List<TaskInfo> tasks = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Timestamp completed = rs.getTimestamp("COMPLETED");
                    tasks.add(new TaskInfo(
                        rs.getLong("SEQUENCE"), rs.getString("IDENTIFIER"),
                        TaskState.ofOrdinal(rs.getInt("STATE")), TaskState.ofOrdinal(rs.getInt("DESCENT")),
                        rs.getString("OWNER"), rs.getString("REFERENCE"),
                        rs.getTimestamp("CREATED").toLocalDateTime().atOffset(ZoneOffset.UTC),
                        completed == null ? null : completed.toLocalDateTime().atOffset(ZoneOffset.UTC),
                        rs.getString("INPUT"), rs.getString("OUTPUT")
                    ));
                }
            }
            return tasks;
        }
    }

    @Override
    public List<TaskInfo> page(
        Connection conn, String topic, Listing listing, long sequence, int size, Direction direction
    ) throws SQLException {
        if (size < 1 || direction.isOutOfBound(sequence)) {
            return Collections.emptyList();
        }
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT SEQUENCE, IDENTIFIER, OWNER, REFERENCE, STATE, DESCENT, CREATED, COMPLETED, INPUT, OUTPUT "
                + "FROM TASK "
                + "WHERE TOPIC = ? "
                + (listing.getState().isPresent() ? "AND STATE = ? " : "")
                + (listing.getIdentifier().isPresent() ? "AND IDENTIFIER = ? " : "")
                + (direction.isLimit(sequence) ? "" : ("AND SEQUENCE " + (direction == Direction.BACKWARD ? "<" : ">") + " ? "))
                + "ORDER BY SEQUENCE " + (direction == Direction.FORWARD ? "" : "DESC ")
                + "FETCH FIRST ? ROWS ONLY"
        )) {
            int index = 0;
            ps.setString(++index, topic);
            if (listing.getState().isPresent()) {
                ps.setInt(++index, listing.getState().orElseThrow().ordinal());
            }
            if (listing.getIdentifier().isPresent()) {
                ps.setString(++index, listing.getIdentifier().orElseThrow());
            }
            if (!direction.isLimit(sequence)) {
                ps.setLong(++index, sequence);
            }
            ps.setInt(++index, size);
            List<TaskInfo> tasks = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Timestamp completed = rs.getTimestamp("COMPLETED");
                    tasks.add(new TaskInfo(
                        rs.getLong("SEQUENCE"), rs.getString("IDENTIFIER"),
                        TaskState.ofOrdinal(rs.getInt("STATE")), TaskState.ofOrdinal(rs.getInt("DESCENT")),
                        rs.getString("OWNER"), rs.getString("REFERENCE"),
                        rs.getTimestamp("CREATED").toLocalDateTime().atOffset(ZoneOffset.UTC),
                        completed == null ? null : completed.toLocalDateTime().atOffset(ZoneOffset.UTC),
                        rs.getString("INPUT"), rs.getString("OUTPUT")
                    ));
                }
            }
            return tasks;
        }
    }

    @Override
    public Map<String, Map<TaskState, Summary>> count(Connection conn, Snapshot snapshot, Counting counting, long from, long to) throws SQLException {
        StringBuilder select = new StringBuilder();
        String compound = "WHERE";
        if (snapshot == Snapshot.RECENT) {
            select.append("WHERE (TOPIC, SEQUENCE) IN (SELECT")
                .append(getCountingHint(counting.getIdentifier().isPresent()))
                .append("TOPIC, MAX(SEQUENCE) FROM TASK ");
        }
        if (counting.getTopic().isPresent()) {
            select.append(compound).append(" TOPIC = ? ");
            compound = "AND";
        }
        if (from > INCEPTION || to < Long.MAX_VALUE) {
            select.append(compound).append(" SEQUENCE BETWEEN ? AND ? ");
            compound = "AND";
        }
        if (counting.getIdentifier().isPresent()) {
            select.append(compound).append(" IDENTIFIER = ? ");
            compound = "AND";
        }
        if (snapshot == Snapshot.RECENT) {
            select.append("GROUP BY TOPIC, IDENTIFIER) ");
            compound = "AND";
            if (counting.getTopic().isPresent()) {
                select.append(compound).append(" TOPIC = ? ");
            }
        }
        if (counting.getState().isPresent()) {
            select.append(compound).append(" STATE = ? ");
        }
        Map<String, Map<TaskState, Summary>> count = new HashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT" + getCountingHint(false) + "TOPIC, STATE, COUNT(*) AS AMOUNT, MAX(SEQUENCE) AS BOUND "
                + "FROM TASK "
                + select
                + "GROUP BY TOPIC, STATE "
                + "UNION ALL "
                + "SELECT TOPIC, -1, 0, 0 "
                + "FROM TASK_TOPIC"
        )) {
            int index = 0;
            if (counting.getTopic().isPresent()) {
                ps.setString(++index, counting.getTopic().orElseThrow());
            }
            if (from > INCEPTION || to < Long.MAX_VALUE) {
                ps.setLong(++index, from);
                ps.setLong(++index, to);
            }
            if (counting.getIdentifier().isPresent()) {
                ps.setString(++index, counting.getIdentifier().orElseThrow());
            }
            if (snapshot == Snapshot.RECENT && counting.getTopic().isPresent()) {
                ps.setString(++index, counting.getTopic().orElseThrow());
            }
            if (counting.getState().isPresent()) {
                ps.setInt(++index, counting.getState().orElseThrow().ordinal());
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int ordinal = rs.getInt("STATE");
                    count.merge(
                        rs.getString("TOPIC"),
                        ordinal == -1
                            ? Collections.emptyMap()
                            : Collections.singletonMap(TaskState.ofOrdinal(ordinal), new Summary(rs.getLong("AMOUNT"), rs.getLong("BOUND"))),
                        (left, right) -> Stream.concat(
                            left.entrySet().stream(),
                            right.entrySet().stream()
                        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                    );
                }
            }
        }
        counting.getTopic().ifPresent(topic -> count.keySet().retainAll(Collections.singleton(topic)));
        return count;
    }

    String getCountingHint(boolean identified) {
        return " ";
    }

    @Override
    public Map<String, Map<TaskResult, Long>> results(
        Connection conn, OffsetDateTime from, OffsetDateTime to
    ) throws SQLException {
        String where;
        if (!from.equals(OffsetDateTime.MIN) && !to.equals(OffsetDateTime.MAX)) {
            where = "AND COMPLETED BETWEEN ? AND ? ";
        } else if (!from.equals(OffsetDateTime.MIN)) {
            where = "AND COMPLETED >= ? ";
        } else if (!to.equals(OffsetDateTime.MAX)) {
            where = "AND COMPLETED <= ? ";
        } else {
            where = "";
        }
        Map<String, Map<TaskResult, Long>> results = new HashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT" + getResultsHint() + "TOPIC, STATE, COUNT(*) AS AMOUNT "
                + "FROM TASK "
                + "WHERE STATE BETWEEN " + TaskResult.SUCCESS.toState().ordinal() + " AND " + TaskResult.FAILURE.toState().ordinal() + " "
                + where
                + "GROUP BY TOPIC, STATE "
                + "UNION ALL "
                + "SELECT TOPIC, -1, 0 "
                + "FROM TASK_TOPIC"
        )) {
            int index = 0;
            if (!from.equals(OffsetDateTime.MIN)) {
                ps.setTimestamp(++index, Timestamp.valueOf(from.atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime()));
            }
            if (!to.equals(OffsetDateTime.MAX)) {
                ps.setTimestamp(++index, Timestamp.valueOf(to.atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime()));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int ordinal = rs.getInt("STATE");
                    results.merge(
                        rs.getString("TOPIC"),
                        ordinal == -1
                            ? Collections.emptyMap()
                            : Collections.singletonMap(TaskResult.ofOrdinal(ordinal - TaskState.SUCCEEDED.ordinal()), rs.getLong("AMOUNT")),
                        (left, right) -> Stream.concat(
                            left.entrySet().stream(),
                            right.entrySet().stream()
                        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                    );
                }
            }
        }
        return results;
    }

    String getResultsHint() {
        return " ";
    }

    @Override
    public void purgeAll(Connection conn, String topic) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
            "DELETE FROM TASK WHERE TOPIC = ?"
        )) {
            ps.setString(1, topic);
            ps.executeLargeUpdate();
        }
    }

    @Override
    public int purgeOwners(Connection conn, long timeout, TimeUnit unit) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
            "DELETE FROM TASK_OWNER "
                + "WHERE OWNER != ? "
                + "AND HEARTBEAT < CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) - INTERVAL '" + unit.toSeconds(timeout) + "' SECOND "
                + "AND OWNER NOT IN (SELECT OWNER FROM TASK)"
        )) {
            ps.setString(1, owner);
            return ps.executeUpdate();
        }
    }

    @Override
    public long purge(Connection conn, String topic, long from, long to) throws SQLException {
        return doPurge(conn, topic, from, to, TaskState.SUCCEEDED, TaskState.REDUNDANT);
    }

    @Override
    public long purge(Connection conn, String topic, TaskState state, long from, long to) throws SQLException {
        return doPurge(conn, topic, from, to, state, state);
    }

    long doPurge(Connection conn, String topic, long from, long to, TaskState lower, TaskState upper) throws SQLException {
        if (to == INCEPTION) {
            return 0;
        }
        try (PreparedStatement ps = conn.prepareStatement(
            "DELETE FROM TASK "
                + "WHERE TOPIC = ? "
                + "AND STATE BETWEEN ? AND ?"
                + (from == INCEPTION && to == Long.MAX_VALUE ? "" : " AND SEQUENCE BETWEEN ? AND ?")
        )) {
            ps.setString(1, topic);
            ps.setInt(2, lower.ordinal());
            ps.setInt(3, upper.ordinal());
            if (from != INCEPTION || to != Long.MAX_VALUE) {
                ps.setLong(4, from);
                ps.setLong(5, to);
            }
            return ps.executeLargeUpdate();
        }
    }

    @Override
    public long resolve(Connection conn, OffsetDateTime dateTime, boolean preceding) throws SQLException {
        if (dateTime.equals(OffsetDateTime.MIN)) {
            return INCEPTION;
        } else if (dateTime.equals(OffsetDateTime.MAX)) {
            return Long.MAX_VALUE;
        }
        try (PreparedStatement ps = conn.prepareStatement(
            "SELECT COALESCE(" + (preceding ? "MAX" : "MIN") + "(SEQUENCE), ?) AS SEQUENCE "
                + "FROM TASK "
                + "WHERE CREATED " + (preceding ? "<=" : ">=") + " ?"
        )) {
            ps.setLong(1, preceding ? INCEPTION : Long.MAX_VALUE);
            ps.setTimestamp(2, Timestamp.valueOf(dateTime.atZoneSameInstant(ZoneOffset.UTC).toLocalDateTime()));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong("SEQUENCE") : (preceding ? INCEPTION : Long.MAX_VALUE);
            }
        }
    }

    static class WithIntervalMultiplication extends JdbcTaskRepository {

        WithIntervalMultiplication(boolean concurrent, String owner) {
            super(concurrent, owner);
        }

        @Override
        public void expire(Connection conn, long timeout, TimeUnit unit) throws SQLException {
            if (timeout < 0) {
                throw new IllegalArgumentException("Cannot accept negative timeout");
            }
            try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE TASK SET STATE = ? "
                    + (concurrent ? "WHERE (TOPIC, SEQUENCE) IN (SELECT TOPIC, SEQUENCE FROM TASK " : "")
                    + "WHERE STATE = ? "
                    + "AND OWNER IN ("
                    + "SELECT OWNER "
                    + "FROM TASK_OWNER "
                    + "WHERE HEARTBEAT < CAST((CURRENT_TIMESTAMP AT TIME ZONE 'UTC') AS TIMESTAMP) - (? * INTERVAL '1' SECOND) "
                    + "AND OWNER != ?"
                    + ")"
                    + (concurrent ? " FOR UPDATE SKIP LOCKED)" : "")
            )) {
                ps.setInt(1, TaskState.EXPIRED.ordinal());
                ps.setInt(2, TaskState.ACTIVE.ordinal());
                ps.setLong(3, unit.toSeconds(timeout));
                ps.setString(4, owner);
                ps.executeUpdate();
            }
        }
    }
}
