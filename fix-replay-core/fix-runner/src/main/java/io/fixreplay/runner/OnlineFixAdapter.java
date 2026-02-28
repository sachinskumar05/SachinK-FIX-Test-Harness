package io.fixreplay.runner;

import io.fixreplay.model.FixMessage;

public interface OnlineFixAdapter {
    String name();

    void send(FixMessage message);
}
