package io.fixreplay.adapter.artio;

import io.fixreplay.model.FixMessage;
import io.fixreplay.runner.OnlineFixAdapter;
import java.util.ArrayList;
import java.util.List;

public final class ArtioOnlineAdapter implements OnlineFixAdapter {
    private final List<FixMessage> sentMessages = new ArrayList<>();

    @Override
    public String name() {
        return "artio";
    }

    @Override
    public void send(FixMessage message) {
        // Placeholder for Artio integration wiring.
        sentMessages.add(message);
    }

    public int sentCount() {
        return sentMessages.size();
    }
}
