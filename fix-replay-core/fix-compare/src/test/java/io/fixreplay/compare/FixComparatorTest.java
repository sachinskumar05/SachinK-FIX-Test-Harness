package io.fixreplay.compare;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.fixreplay.model.FixMessage;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class FixComparatorTest {
    @Test
    void ignoredTagsDoNotFailComparison() {
        FixMessage expected = FixMessage.fromRaw("8=FIX.4.4|35=8|11=ORD-1|60=20260228-10:00:00.000|10=001|", '|');
        FixMessage actual = FixMessage.fromRaw("8=FIX.4.4|35=8|11=ORD-1|60=20260228-10:05:00.000|10=999|", '|');

        CompareConfig config = CompareConfig.builder()
            .excludeTimeLikeTags(true)
            .build();
        CompareResult result = new FixComparator().compare(expected, actual, config);

        assertTrue(result.passed());
        assertTrue(result.differingValues().isEmpty());
    }

    @Test
    void differingValueProducesDiffEntry() {
        FixMessage expected = FixMessage.fromRaw("35=8|11=ORD-1|39=2|", '|');
        FixMessage actual = FixMessage.fromRaw("35=8|11=ORD-1|39=8|", '|');

        CompareConfig config = CompareConfig.builder()
            .defaultExcludeTags(Set.of())
            .build();
        CompareResult result = new FixComparator().compare(expected, actual, config);

        assertFalse(result.passed());
        assertEquals(Map.of(39, new CompareResult.ValueDifference("2", "8")), result.differingValues());
    }

    @Test
    void tagNormalizerCanMakeComparisonPass() {
        FixMessage expected = FixMessage.fromRaw("35=8|37=RA-12345 |", '|');
        FixMessage actual = FixMessage.fromRaw("35=8|37=12345|", '|');

        CompareConfig config = CompareConfig.builder()
            .defaultExcludeTags(Set.of())
            .normalizer(
                37,
                CompareConfig.TagNormalizer.builder()
                    .trim(true)
                    .regexReplace("^RA-", "")
                    .build()
            )
            .build();
        CompareResult result = new FixComparator().compare(expected, actual, config);

        assertTrue(result.passed());
    }

    @Test
    void diffReportProducesJsonAndJUnitXml() {
        CompareResult passing = new CompareResult("8", List.of(), List.of(), Map.of());
        CompareResult failing = new CompareResult(
            "8",
            List.of(54),
            List.of(150),
            Map.of(39, new CompareResult.ValueDifference("2", "8"))
        );
        DiffReport report = new DiffReport(
            List.of(
                new DiffReport.MessageResult("msg-1", passing),
                new DiffReport.MessageResult("msg-2", failing)
            )
        );

        String json = report.toJson();
        String junitXml = report.toJUnitXml("fix-compare-suite");

        assertTrue(json.contains("\"failedMessages\":1"));
        assertTrue(json.contains("\"msgType\":\"8\""));
        assertTrue(junitXml.contains("<testsuite name=\"fix-compare-suite\""));
        assertTrue(junitXml.contains("<failure message=\"Comparison failed\">"));
    }
}
