package io.fixreplay.loader;

import io.fixreplay.model.FixMessage;
import java.util.ArrayList;
import java.util.List;

public final class FixLogScanner {
    public List<FixLogEntry> extract(List<String> lines, char delimiter) {
        List<FixLogEntry> entries = new ArrayList<>();
        for (int index = 0; index < lines.size(); index++) {
            String line = lines.get(index);
            int fixStart = line.indexOf("8=FIX");
            if (fixStart < 0) {
                continue;
            }
            String rawMessage = line.substring(fixStart).trim();
            entries.add(new FixLogEntry(index + 1L, FixMessage.fromRaw(rawMessage, delimiter)));
        }
        return List.copyOf(entries);
    }
}
