package io.fixreplay.linker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fixreplay.loader.FixLogScanner;
import java.util.List;
import org.junit.jupiter.api.Test;

class DeterministicLinkerTest {
    @Test
    void linksRequestAndExecutionReportByClOrdId() {
        FixLogScanner scanner = new FixLogScanner();
        DeterministicLinker linker = new DeterministicLinker();

        List<FixLink> links = linker.link(
            scanner.extract(
                List.of(
                    "8=FIX.4.2|35=D|11=ABC123|",
                    "8=FIX.4.2|35=8|11=ABC123|"
                ),
                '|'
            )
        );

        assertEquals(1, links.size());
        assertEquals("ABC123", links.get(0).correlationId());
    }
}
