package io.fixreplay.linker;

import io.fixreplay.loader.FixLogEntry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class DeterministicLinker {
    private final LinkerConfig config;

    public DeterministicLinker() {
        this(LinkerConfig.defaults());
    }

    public DeterministicLinker(LinkerConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

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

    public LinkReport link(List<FixLogEntry> inMessages, List<FixLogEntry> outMessages) {
        LinkDiscovery discovery = new LinkDiscovery(config);
        Map<String, List<Integer>> strategyByMsgType = discovery.discover(inMessages, outMessages);
        LinkIndex index = LinkIndex.build(inMessages, strategyByMsgType, config);
        LinkIndex.Result result = index.link(outMessages);
        return new LinkReport(
            strategyByMsgType,
            result.matches(),
            result.unmatched(),
            result.ambiguous(),
            result.topCollisions()
        );
    }

    private static boolean isRequest(String msgType) {
        return "D".equals(msgType) || "F".equals(msgType) || "G".equals(msgType);
    }

    private static boolean isResponse(String msgType) {
        return "8".equals(msgType) || "9".equals(msgType);
    }
}
