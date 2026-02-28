package io.fixreplay.loader;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class FixLogScannerTest {
    @Test
    void extractsOnlyFixMessages() {
        FixLogScanner scanner = new FixLogScanner();

        List<FixLogEntry> entries = scanner.extract(
            List.of(
                "INFO - connected",
                "2026-02-28 10:00:00 8=FIX.4.2|35=D|11=ABC123|",
                "2026-02-28 10:00:01 8=FIX.4.2|35=8|11=ABC123|"
            ),
            '|'
        );

        assertEquals(2, entries.size());
        assertEquals("D", entries.get(0).message().get(35));
    }
}
