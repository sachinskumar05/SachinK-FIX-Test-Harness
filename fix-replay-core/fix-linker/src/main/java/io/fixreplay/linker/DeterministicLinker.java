package io.fixreplay.linker;

import io.fixreplay.loader.FixLogEntry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class DeterministicLinker {
    public List<FixLink> link(List<FixLogEntry> entries) {
        Map<String, Long> pendingRequests = new LinkedHashMap<>();
        List<FixLink> links = new ArrayList<>();

        for (FixLogEntry entry : entries) {
            String msgType = entry.message().get(35);
            String clOrdId = entry.message().get(11);
            if (msgType == null || clOrdId == null) {
                continue;
            }

            if (isRequest(msgType)) {
                pendingRequests.putIfAbsent(clOrdId, entry.lineNumber());
                continue;
            }

            if (isResponse(msgType)) {
                Long requestLine = pendingRequests.remove(clOrdId);
                if (requestLine != null) {
                    links.add(new FixLink(clOrdId, requestLine, entry.lineNumber()));
                }
            }
        }

        return List.copyOf(links);
    }

    private static boolean isRequest(String msgType) {
        return "D".equals(msgType) || "F".equals(msgType) || "G".equals(msgType);
    }

    private static boolean isResponse(String msgType) {
        return "8".equals(msgType) || "9".equals(msgType);
    }
}
