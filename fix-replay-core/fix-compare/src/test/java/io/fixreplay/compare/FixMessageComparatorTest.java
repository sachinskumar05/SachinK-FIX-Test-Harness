package io.fixreplay.compare;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fixreplay.model.FixMessage;
import java.util.Set;
import org.junit.jupiter.api.Test;

class FixMessageComparatorTest {
    @Test
    void findsTagDifferencesWhileIgnoringConfiguredTags() {
        FixMessage expected = FixMessage.fromRaw("35=D|11=ABC123|55=MSFT|", '|');
        FixMessage actual = FixMessage.fromRaw("35=D|11=ABC123|55=AAPL|", '|');

        FixMessageComparator comparator = new FixMessageComparator();
        var diffs = comparator.compare(expected, actual, Set.of(11));

        assertEquals(1, diffs.size());
        assertEquals(55, diffs.get(0).tag());
    }
}
