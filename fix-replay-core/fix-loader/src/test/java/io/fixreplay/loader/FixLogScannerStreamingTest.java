package io.fixreplay.loader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.fixreplay.model.FixMessage;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FixLogScannerStreamingTest {
    private static final char SOH = '\u0001';

    @Test
    void scansCaretDelimitedMessagesAndExtractsMetadata() {
        FixLogScanner scanner = new FixLogScanner(new LogScanConfig(64, 8 * 1024, LogScanConfig.defaults().delimiterRules()));

        List<FixRawMessage> messages;
        try (Stream<FixRawMessage> stream = scanner.scan(resourcePath("sample1-caret.log"))) {
            messages = stream.toList();
        }

        assertEquals(2, messages.size());

        FixMessage first = messages.get(0).toFixMessage();
        assertEquals("FIX.4.4", first.get(8));
        assertEquals("D", first.get(35));
        assertEquals("128", first.get(10));
        assertEquals("2026-02-28 10:00:00.123", messages.get(0).timestamp().orElseThrow());
        assertEquals(FixRawMessage.Direction.IN, messages.get(0).direction().orElseThrow());

        FixMessage second = messages.get(1).toFixMessage();
        assertEquals("FIX.4.4", second.get(8));
        assertEquals("8", second.get(35));
        assertEquals("042", second.get(10));
        assertEquals("2026-02-28 10:00:01.456", messages.get(1).timestamp().orElseThrow());
        assertEquals(FixRawMessage.Direction.OUT, messages.get(1).direction().orElseThrow());
    }

    @Test
    void scansBracketedMessagesWithoutBracketArtifacts() {
        FixLogScanner scanner = new FixLogScanner(new LogScanConfig(48, 8 * 1024, LogScanConfig.defaults().delimiterRules()));

        List<FixRawMessage> messages;
        try (Stream<FixRawMessage> stream = scanner.scan(resourcePath("sample2-bracketed.log"))) {
            messages = stream.toList();
        }

        assertEquals(2, messages.size());
        assertFalse(messages.get(0).payloadText().contains("["));
        assertFalse(messages.get(0).payloadText().contains("]"));

        FixMessage first = messages.get(0).toFixMessage();
        assertEquals("FIX.4.2", first.get(8));
        assertEquals("F", first.get(35));
        assertEquals("231", first.get(10));

        FixMessage second = messages.get(1).toFixMessage();
        assertEquals("FIX.4.2", second.get(8));
        assertEquals("9", second.get(35));
        assertEquals("777", second.get(10));
    }

    @Test
    void scansMixedNoiseAndPipeDelimitedMessages() {
        FixLogScanner scanner = new FixLogScanner(new LogScanConfig(64, 8 * 1024, LogScanConfig.defaults().delimiterRules()));

        List<FixRawMessage> messages;
        try (Stream<FixRawMessage> stream = scanner.scan(resourcePath("sample3-mixed-noise.log"))) {
            messages = stream.toList();
        }

        assertEquals(2, messages.size());

        FixMessage first = messages.get(0).toFixMessage();
        assertEquals("FIX.4.4", first.get(8));
        assertEquals("0", first.get(35));
        assertEquals("001", first.get(10));

        FixMessage second = messages.get(1).toFixMessage();
        assertEquals("FIX.4.4", second.get(8));
        assertEquals("A", second.get(35));
        assertEquals("222", second.get(10));
    }

    @Test
    void scansSohDelimitedPayloads(@TempDir Path tempDir) throws IOException {
        Path input = tempDir.resolve("sample-soh.log");
        String content = "2026-02-28 10:05:00.000 IN 8=FIX.4.4" + SOH + "9=056" + SOH + "35=D" + SOH + "11=SOH-1" + SOH + "10=900" + SOH + "\n" +
            "2026-02-28 10:05:01.000 OUT 8=FIX.4.4" + SOH + "9=058" + SOH + "35=8" + SOH + "11=SOH-1" + SOH + "10=901" + SOH + "\n";
        Files.writeString(input, content, StandardCharsets.ISO_8859_1);

        FixLogScanner scanner = new FixLogScanner(new LogScanConfig(32, 8 * 1024, LogScanConfig.defaults().delimiterRules()));
        List<FixRawMessage> messages;
        try (Stream<FixRawMessage> stream = scanner.scan(input)) {
            messages = stream.toList();
        }

        assertEquals(2, messages.size());
        assertEquals("D", messages.get(0).toFixMessage().get(35));
        assertEquals("900", messages.get(0).toFixMessage().get(10));
        assertEquals("8", messages.get(1).toFixMessage().get(35));
        assertEquals("901", messages.get(1).toFixMessage().get(10));
    }

    @Test
    void scansLargeSyntheticFileQuickly(@TempDir Path tempDir) throws IOException {
        Path input = tempDir.resolve("synthetic-100k.log");
        int messageCount = 100_000;

        try (BufferedWriter writer = Files.newBufferedWriter(input, StandardCharsets.US_ASCII)) {
            for (int i = 0; i < messageCount; i++) {
                writer.write("2026-02-28 11:00:00.000 OUT 8=FIX.4.4|9=120|35=D|11=ORD");
                writer.write(Integer.toString(i));
                writer.write("|55=AAPL|54=1|38=100|44=182.35|10=123|");
                writer.newLine();
                if ((i & 31) == 0) {
                    writer.write("noise line ");
                    writer.write(Integer.toString(i));
                    writer.newLine();
                }
            }
        }

        FixLogScanner scanner = new FixLogScanner(new LogScanConfig(4 * 1024, 512 * 1024, LogScanConfig.defaults().delimiterRules()));
        long started = System.nanoTime();
        long scannedCount;
        try (Stream<FixRawMessage> stream = scanner.scan(input)) {
            scannedCount = stream.count();
        }
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

        assertEquals(messageCount, scannedCount);
        assertTrue(elapsedMs < 45_000, "Expected scan under 45000ms, but took " + elapsedMs + "ms");
    }

    private static Path resourcePath(String fileName) {
        try {
            return Path.of(
                Objects.requireNonNull(
                    FixLogScannerStreamingTest.class.getResource("/io/fixreplay/loader/" + fileName),
                    "Missing test resource " + fileName
                ).toURI()
            );
        } catch (URISyntaxException ex) {
            throw new IllegalStateException("Invalid resource URI for " + fileName, ex);
        }
    }
}
