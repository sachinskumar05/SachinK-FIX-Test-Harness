package io.fixreplay.adapter.artio;

import io.fixreplay.model.FixMessage;
import java.util.function.Consumer;

interface ArtioGateway extends AutoCloseable {
    void setReceiveHandler(Consumer<FixMessage> callback);

    void connect(ArtioTransportConfig config);

    void send(FixMessage message);

    @Override
    void close();
}

