package io.fixreplay.linker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.fixreplay.loader.FixLogEntry;
import io.fixreplay.model.FixMessage;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LinkDiscoveryTest {
    @Test
    void discoversBestKeysAndLinksInOutDeterministically() {
        DeterministicLinker linker = new DeterministicLinker();

        LinkReport report = linker.link(inFixtures(), outFixtures());

        assertEquals(List.of(11), report.strategyByMsgType().get("D"));
        assertEquals(List.of(41), report.strategyByMsgType().get("G"));
        assertEquals(List.of(17, 37), report.strategyByMsgType().get("8"));

        assertEquals(6, report.matched());
        assertEquals(2, report.unmatched());
        assertEquals(1, report.ambiguous());

        assertFalse(report.topCollisions().isEmpty());
        LinkReport.CollisionExample collision = report.topCollisions().get(0);
        assertEquals("G", collision.msgType());
        assertEquals(List.of(41), collision.tags());
        assertEquals("41=ORD-2", collision.key());
        assertEquals(2, collision.inCount());
        assertEquals(List.of(4L, 5L), collision.inLines());
    }

    @Test
    void producesStableJsonAcrossRepeatedRuns() {
        DeterministicLinker linker = new DeterministicLinker();
        String baselineJson = linker.link(inFixtures(), outFixtures()).toJson();
        for (int i = 0; i < 20; i++) {
            String rerunJson = linker.link(inFixtures(), outFixtures()).toJson();
            assertEquals(baselineJson, rerunJson);
        }
    }

    @Test
    void appliesTagNormalizersDuringDiscoveryAndLinking() {
        LinkerConfig config = LinkerConfig.builder()
            .candidateTags(List.of(41))
            .candidateCombinations(List.of(List.of(41)))
            .overrideCandidates("G", List.of(List.of(41)))
            .normalizer(
                41,
                LinkerConfig.TagNormalizer.builder()
                    .trim(true)
                    .regexReplace("^ID-", "")
                    .build()
            )
            .build();
        DeterministicLinker linker = new DeterministicLinker(config);

        List<FixLogEntry> in = List.of(
            entry(1, "8=FIX.4.4|35=G|41=ID-ORD-1 |10=001|")
        );
        List<FixLogEntry> out = List.of(
            entry(10, "8=FIX.4.4|35=G|41=ORD-1|10=111|")
        );

        LinkReport report = linker.link(in, out);

        assertEquals(Map.of("G", List.of(41)), report.strategyByMsgType());
        assertEquals(1, report.matched());
        assertEquals(0, report.unmatched());
        assertEquals(0, report.ambiguous());
    }

    private static List<FixLogEntry> inFixtures() {
        return List.of(
            entry(1, "8=FIX.4.4|35=D|11=ORD-1|55=AAPL|54=1|60=20260228-10:00:00.000|10=001|"),
            entry(2, "8=FIX.4.4|35=D|11=ORD-2|55=AAPL|54=1|60=20260228-10:00:00.001|10=002|"),
            entry(3, "8=FIX.4.4|35=G|41=ORD-1|11=RPL-1|55=AAPL|54=1|10=003|"),
            entry(4, "8=FIX.4.4|35=G|41=ORD-2|11=RPL-2|55=AAPL|54=1|10=004|"),
            entry(5, "8=FIX.4.4|35=G|41=ORD-2|11=RPL-3|55=AAPL|54=1|10=005|"),
            entry(6, "8=FIX.4.4|35=8|37=EX-100|17=E-1|11=ORD-1|10=006|"),
            entry(7, "8=FIX.4.4|35=8|37=EX-100|17=E-2|11=ORD-1|10=007|"),
            entry(8, "8=FIX.4.4|35=8|37=EX-200|17=E-1|11=ORD-2|10=008|")
        );
    }

    private static List<FixLogEntry> outFixtures() {
        return List.of(
            entry(101, "8=FIX.4.4|35=D|11=ORD-1|10=101|"),
            entry(102, "8=FIX.4.4|35=D|11=ORD-2|10=102|"),
            entry(103, "8=FIX.4.4|35=G|41=ORD-1|10=103|"),
            entry(104, "8=FIX.4.4|35=G|41=ORD-2|10=104|"),
            entry(105, "8=FIX.4.4|35=G|41=ORD-X|10=105|"),
            entry(106, "8=FIX.4.4|35=8|37=EX-100|17=E-1|10=106|"),
            entry(107, "8=FIX.4.4|35=8|37=EX-100|17=E-2|10=107|"),
            entry(108, "8=FIX.4.4|35=8|37=EX-200|17=E-1|10=108|"),
            entry(109, "8=FIX.4.4|35=8|37=EX-999|17=E-9|10=109|")
        );
    }

    private static FixLogEntry entry(long lineNumber, String raw) {
        return new FixLogEntry(lineNumber, FixMessage.fromRaw(raw, '|'));
    }
}
