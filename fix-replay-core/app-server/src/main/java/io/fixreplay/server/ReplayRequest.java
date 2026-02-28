package io.fixreplay.server;

import java.util.List;

public record ReplayRequest(List<String> expectedLines, List<String> actualLines) {
}
