package io.fixreplay.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

final class InMemoryJobStore implements AutoCloseable {
    private final ObjectMapper objectMapper;
    private final ExecutorService executor;
    private final ConcurrentHashMap<String, JobRecord> jobs = new ConcurrentHashMap<>();

    InMemoryJobStore(ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread thread = new Thread(r, "fix-replay-job-worker");
            thread.setDaemon(true);
            return thread;
        });
    }

    String submit(String name, Supplier<Object> task) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(task, "task");

        String jobId = UUID.randomUUID().toString();
        JobRecord record = JobRecord.queued(jobId, name);
        jobs.put(jobId, record);

        executor.submit(() -> {
            record.markRunning();
            try {
                Object result = task.get();
                JsonNode resultNode = objectMapper.valueToTree(result);
                record.markSucceeded(resultNode);
            } catch (Throwable failure) {
                record.markFailed(failure);
            }
        });
        return jobId;
    }

    Optional<Map<String, Object>> get(String jobId) {
        JobRecord record = jobs.get(jobId);
        if (record == null) {
            return Optional.empty();
        }
        return Optional.of(record.snapshot());
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    private enum JobStatus {
        QUEUED,
        RUNNING,
        SUCCEEDED,
        FAILED
    }

    private static final class JobRecord {
        private final String jobId;
        private final String name;
        private final Instant submittedAt;

        private volatile JobStatus status;
        private volatile Instant startedAt;
        private volatile Instant finishedAt;
        private volatile JsonNode result;
        private volatile String error;

        private JobRecord(String jobId, String name, Instant submittedAt, JobStatus status) {
            this.jobId = jobId;
            this.name = name;
            this.submittedAt = submittedAt;
            this.status = status;
        }

        static JobRecord queued(String jobId, String name) {
            return new JobRecord(jobId, name, Instant.now(), JobStatus.QUEUED);
        }

        synchronized void markRunning() {
            if (status != JobStatus.QUEUED) {
                return;
            }
            status = JobStatus.RUNNING;
            startedAt = Instant.now();
        }

        synchronized void markSucceeded(JsonNode result) {
            status = JobStatus.SUCCEEDED;
            finishedAt = Instant.now();
            this.result = result;
        }

        synchronized void markFailed(Throwable failure) {
            status = JobStatus.FAILED;
            finishedAt = Instant.now();
            error = failure.getMessage() == null ? failure.getClass().getName() : failure.getMessage();
        }

        Map<String, Object> snapshot() {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("jobId", jobId);
            out.put("name", name);
            out.put("status", status.name());
            out.put("submittedAt", submittedAt.toString());
            out.put("startedAt", startedAt == null ? null : startedAt.toString());
            out.put("finishedAt", finishedAt == null ? null : finishedAt.toString());
            out.put("result", result);
            out.put("error", error);
            return out;
        }
    }
}

