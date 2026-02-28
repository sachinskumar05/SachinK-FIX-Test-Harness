package io.fixreplay.adapter.quickfixj;

import io.fixreplay.model.FixMessage;
import io.fixreplay.runner.OnlineFixAdapter;
import java.util.ArrayList;
import java.util.List;

public final class QuickFixJOnlineAdapter implements OnlineFixAdapter {
    private final List<FixMessage> sentMessages = new ArrayList<>();

    @Override
    public String name() {
        return "quickfixj";
    }

    @Override
    public void send(FixMessage message) {
        // Placeholder for QuickFIX/J integration wiring.
        sentMessages.add(message);
    }

    public int sentCount() {
        return sentMessages.size();
    }
}
