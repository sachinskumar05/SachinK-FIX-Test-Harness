package io.fixreplay.runner;

import io.fixreplay.model.FixMessage;
import java.util.function.Consumer;

public interface FixTransport extends AutoCloseable {
    void connect(TransportSessionConfig sessionConfig);

    void onReceive(Consumer<FixMessage> callback);

    void send(FixMessage message);

    @Override
    void close();
}
