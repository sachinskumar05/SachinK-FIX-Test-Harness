package io.fixreplay.loader;

import io.fixreplay.model.FixMessage;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class FixLogScanner {
    private static final byte[] FIX_START_MARKER = "8=FIX".getBytes(StandardCharsets.US_ASCII);
    private static final byte SOH = 0x01;

    private final LogScanConfig config;

    public FixLogScanner() {
        this(LogScanConfig.defaults());
    }

    public FixLogScanner(LogScanConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    public List<FixLogEntry> extract(List<String> lines, char delimiter) {
        List<FixLogEntry> entries = new ArrayList<>();
        for (int index = 0; index < lines.size(); index++) {
            String line = lines.get(index);
            int fixStart = line.indexOf("8=FIX");
            if (fixStart < 0) {
                continue;
            }
            int messageEnd = findLegacyMessageEnd(line, fixStart, delimiter);
            String rawMessage = (messageEnd > fixStart ? line.substring(fixStart, messageEnd) : line.substring(fixStart)).trim();
            entries.add(new FixLogEntry(index + 1L, FixMessage.fromRaw(rawMessage, delimiter)));
        }
        return List.copyOf(entries);
    }

    public Stream<FixRawMessage> scan(Path path) {
        Objects.requireNonNull(path, "path");
        final StreamingIterator iterator;
        try {
            iterator = new StreamingIterator(path, config);
        } catch (IOException ex) {
            throw new UncheckedIOException("Failed to open FIX log: " + path, ex);
        }

        Spliterator<FixRawMessage> spliterator =
            Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false).onClose(iterator::close);
    }

    private static int findLegacyMessageEnd(String line, int startIndex, char delimiter) {
        int searchFrom = startIndex + FIX_START_MARKER.length;
        for (int i = searchFrom; i + 5 < line.length(); i++) {
            if (
                line.charAt(i) == '1' &&
                line.charAt(i + 1) == '0' &&
                line.charAt(i + 2) == '=' &&
                Character.isDigit(line.charAt(i + 3)) &&
                Character.isDigit(line.charAt(i + 4)) &&
                Character.isDigit(line.charAt(i + 5)) &&
                i > startIndex &&
                line.charAt(i - 1) == delimiter
            ) {
                int afterChecksum = i + 6;
                if (afterChecksum >= line.length()) {
                    return afterChecksum;
                }
                char next = line.charAt(afterChecksum);
                if (next == delimiter) {
                    return afterChecksum + 1;
                }
                if (Character.isWhitespace(next) || next == ']' || next == ')') {
                    return afterChecksum;
                }
            }
        }
        return -1;
    }

    private static Optional<String> extractTimestamp(String prefix) {
        for (int i = 0; i + 18 < prefix.length(); i++) {
            if (looksLikeTimestamp(prefix, i)) {
                int end = i + 19;
                while (end < prefix.length() && isTimestampTail(prefix.charAt(end))) {
                    end++;
                }
                return Optional.of(prefix.substring(i, end));
            }
        }
        return Optional.empty();
    }

    private static boolean looksLikeTimestamp(String input, int index) {
        return hasDigits(input, index, 4) &&
            input.charAt(index + 4) == '-' &&
            hasDigits(input, index + 5, 2) &&
            input.charAt(index + 7) == '-' &&
            hasDigits(input, index + 8, 2) &&
            (input.charAt(index + 10) == ' ' || input.charAt(index + 10) == 'T') &&
            hasDigits(input, index + 11, 2) &&
            input.charAt(index + 13) == ':' &&
            hasDigits(input, index + 14, 2) &&
            input.charAt(index + 16) == ':' &&
            hasDigits(input, index + 17, 2);
    }

    private static boolean hasDigits(String input, int index, int count) {
        for (int i = 0; i < count; i++) {
            if (!Character.isDigit(input.charAt(index + i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isTimestampTail(char value) {
        return Character.isDigit(value) ||
            value == '.' ||
            value == ',' ||
            value == ':' ||
            value == '+' ||
            value == '-' ||
            value == 'Z';
    }

    private static Optional<FixRawMessage.Direction> extractDirection(String prefix) {
        String upper = prefix.toUpperCase(Locale.ROOT);
        int inIndex = findToken(upper, "IN");
        int outIndex = findToken(upper, "OUT");
        if (inIndex < 0 && outIndex < 0) {
            return Optional.empty();
        }
        if (inIndex >= 0 && outIndex >= 0) {
            return inIndex > outIndex ? Optional.of(FixRawMessage.Direction.IN) : Optional.of(FixRawMessage.Direction.OUT);
        }
        return inIndex >= 0 ? Optional.of(FixRawMessage.Direction.IN) : Optional.of(FixRawMessage.Direction.OUT);
    }

    private static int findToken(String text, String token) {
        int max = text.length() - token.length();
        for (int i = 0; i <= max; i++) {
            if (text.regionMatches(i, token, 0, token.length()) && isWordBoundary(text, i - 1) && isWordBoundary(text, i + token.length())) {
                return i;
            }
        }
        return -1;
    }

    private static boolean isWordBoundary(String text, int index) {
        if (index < 0 || index >= text.length()) {
            return true;
        }
        return !Character.isLetterOrDigit(text.charAt(index));
    }

    private static TerminatorScan findChecksumTerminator(
        byte[] data,
        int length,
        int fromIndex,
        boolean eof,
        LogScanConfig config
    ) {
        int i = Math.max(0, fromIndex);
        while (i + 5 < length) {
            if (
                data[i] == '1' &&
                data[i + 1] == '0' &&
                data[i + 2] == '=' &&
                isDigit(data[i + 3]) &&
                isDigit(data[i + 4]) &&
                isDigit(data[i + 5]) &&
                hasFieldBoundaryBefore(data, i, config)
            ) {
                int afterChecksum = i + 6;
                if (afterChecksum == length) {
                    if (eof) {
                        return TerminatorScan.found(afterChecksum);
                    }
                    return TerminatorScan.needMore(Math.max(0, i - 2));
                }

                byte next = data[afterChecksum];
                if (config.supports(LogScanConfig.DelimiterRule.SOH) && next == SOH) {
                    return TerminatorScan.found(afterChecksum + 1);
                }
                if (config.supports(LogScanConfig.DelimiterRule.PIPE) && next == '|') {
                    return TerminatorScan.found(afterChecksum + 1);
                }
                if (config.supports(LogScanConfig.DelimiterRule.CARET_A) && next == '^') {
                    if (afterChecksum + 1 < length) {
                        if (data[afterChecksum + 1] == 'A') {
                            return TerminatorScan.found(afterChecksum + 2);
                        }
                    } else if (!eof) {
                        return TerminatorScan.needMore(Math.max(0, i - 2));
                    }
                }
                if (isMessageBoundary(next)) {
                    return TerminatorScan.found(afterChecksum);
                }
            }
            i++;
        }
        return TerminatorScan.notFound(Math.max(0, length - 8));
    }

    private static boolean hasFieldBoundaryBefore(byte[] data, int tagIndex, LogScanConfig config) {
        if (tagIndex <= 0) {
            return false;
        }
        byte previous = data[tagIndex - 1];
        if (config.supports(LogScanConfig.DelimiterRule.SOH) && previous == SOH) {
            return true;
        }
        if (config.supports(LogScanConfig.DelimiterRule.PIPE) && previous == '|') {
            return true;
        }
        return config.supports(LogScanConfig.DelimiterRule.CARET_A) &&
            tagIndex > 1 &&
            data[tagIndex - 2] == '^' &&
            data[tagIndex - 1] == 'A';
    }

    private static boolean isMessageBoundary(byte value) {
        return value == '\n' ||
            value == '\r' ||
            value == ']' ||
            value == ')' ||
            value == ' ' ||
            value == '\t';
    }

    private static boolean isDigit(byte value) {
        return value >= '0' && value <= '9';
    }

    private static byte[] normalizeDelimiters(byte[] data, int length, LogScanConfig config) {
        byte[] out = new byte[length];
        int writeIndex = 0;
        for (int i = 0; i < length; i++) {
            byte value = data[i];
            if (config.supports(LogScanConfig.DelimiterRule.SOH) && value == SOH) {
                out[writeIndex++] = SOH;
                continue;
            }
            if (config.supports(LogScanConfig.DelimiterRule.PIPE) && value == '|') {
                out[writeIndex++] = SOH;
                continue;
            }
            if (
                config.supports(LogScanConfig.DelimiterRule.CARET_A) &&
                value == '^' &&
                i + 1 < length &&
                data[i + 1] == 'A'
            ) {
                out[writeIndex++] = SOH;
                i++;
                continue;
            }
            out[writeIndex++] = value;
        }
        return Arrays.copyOf(out, writeIndex);
    }

    private record TerminatorScan(boolean found, int endExclusive, int nextSearchIndex) {
        static TerminatorScan found(int endExclusive) {
            return new TerminatorScan(true, endExclusive, endExclusive);
        }

        static TerminatorScan needMore(int nextSearchIndex) {
            return new TerminatorScan(false, -1, nextSearchIndex);
        }

        static TerminatorScan notFound(int nextSearchIndex) {
            return new TerminatorScan(false, -1, nextSearchIndex);
        }
    }

    private static final class RollingLineBuffer {
        private final byte[] buffer;
        private int start;
        private int size;

        private RollingLineBuffer(int capacity) {
            this.buffer = new byte[capacity];
        }

        void append(byte value) {
            if (value == '\n' || value == '\r') {
                clear();
                return;
            }
            if (size < buffer.length) {
                buffer[(start + size) % buffer.length] = value;
                size++;
                return;
            }
            buffer[start] = value;
            start = (start + 1) % buffer.length;
        }

        byte[] snapshot() {
            byte[] out = new byte[size];
            if (size == 0) {
                return out;
            }
            int firstCopy = Math.min(size, buffer.length - start);
            System.arraycopy(buffer, start, out, 0, firstCopy);
            if (firstCopy < size) {
                System.arraycopy(buffer, 0, out, firstCopy, size - firstCopy);
            }
            return out;
        }

        void clear() {
            start = 0;
            size = 0;
        }
    }

    private static final class ByteAccumulator {
        private byte[] buffer;
        private int size;

        private ByteAccumulator(int initialCapacity) {
            this.buffer = new byte[Math.max(32, initialCapacity)];
        }

        void append(byte value) {
            ensureCapacity(size + 1);
            buffer[size++] = value;
        }

        void append(byte[] value) {
            ensureCapacity(size + value.length);
            System.arraycopy(value, 0, buffer, size, value.length);
            size += value.length;
        }

        byte[] array() {
            return buffer;
        }

        int size() {
            return size;
        }

        void clear() {
            size = 0;
        }

        private void ensureCapacity(int minCapacity) {
            if (minCapacity <= buffer.length) {
                return;
            }
            int newSize = buffer.length;
            while (newSize < minCapacity) {
                newSize *= 2;
            }
            buffer = Arrays.copyOf(buffer, newSize);
        }
    }

    private static final class StreamingIterator implements Iterator<FixRawMessage>, AutoCloseable {
        private static final int LINE_CONTEXT_LIMIT = 4096;

        private final Path sourceFile;
        private final LogScanConfig config;
        private final FileChannel channel;
        private final ByteBuffer readBuffer;
        private final RollingLineBuffer lineContext;
        private final ByteAccumulator messageBuffer;

        private FixRawMessage nextMessage;
        private boolean inMessage;
        private long absoluteOffset;
        private long messageStartOffset;
        private int startMatchLength;
        private int checksumSearchIndex;
        private boolean completed;
        private boolean closed;

        private Optional<String> currentTimestamp = Optional.empty();
        private Optional<FixRawMessage.Direction> currentDirection = Optional.empty();

        private StreamingIterator(Path sourceFile, LogScanConfig config) throws IOException {
            this.sourceFile = sourceFile;
            this.config = config;
            this.channel = FileChannel.open(sourceFile, StandardOpenOption.READ);
            this.readBuffer = ByteBuffer.allocate(config.chunkSize());
            this.readBuffer.limit(0);
            this.lineContext = new RollingLineBuffer(LINE_CONTEXT_LIMIT);
            this.messageBuffer = new ByteAccumulator(Math.min(config.maxMessageLength(), 4096));
        }

        @Override
        public boolean hasNext() {
            ensureNextLoaded();
            return nextMessage != null;
        }

        @Override
        public FixRawMessage next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            FixRawMessage result = nextMessage;
            nextMessage = null;
            return result;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            try {
                channel.close();
            } catch (IOException ex) {
                throw new UncheckedIOException("Failed to close FIX log scanner for " + sourceFile, ex);
            }
        }

        private void ensureNextLoaded() {
            if (nextMessage != null || completed) {
                return;
            }
            try {
                while (nextMessage == null && !completed) {
                    if (!readBuffer.hasRemaining()) {
                        if (!readNextChunk()) {
                            finishAtEndOfFile();
                            completed = true;
                            closeQuietly();
                            return;
                        }
                    }
                    while (nextMessage == null && readBuffer.hasRemaining()) {
                        byte value = readBuffer.get();
                        processByte(value);
                        absoluteOffset++;
                    }
                }
            } catch (IOException ex) {
                closeQuietly();
                throw new UncheckedIOException("Failed while scanning FIX log " + sourceFile, ex);
            }
        }

        private boolean readNextChunk() throws IOException {
            readBuffer.clear();
            int readCount = channel.read(readBuffer);
            if (readCount < 0) {
                return false;
            }
            readBuffer.flip();
            return true;
        }

        private void processByte(byte value) {
            if (inMessage) {
                processMessageByte(value);
                return;
            }

            lineContext.append(value);
            updateStartMatcher(value);
            if (startMatchLength == FIX_START_MARKER.length) {
                startMessageCapture();
            }
        }

        private void processMessageByte(byte value) {
            messageBuffer.append(value);
            if (messageBuffer.size() > config.maxMessageLength()) {
                resetMessageCapture();
                return;
            }

            TerminatorScan terminator = findChecksumTerminator(
                messageBuffer.array(),
                messageBuffer.size(),
                checksumSearchIndex,
                false,
                config
            );
            checksumSearchIndex = terminator.nextSearchIndex();
            if (terminator.found()) {
                byte[] normalized = normalizeDelimiters(messageBuffer.array(), terminator.endExclusive(), config);
                nextMessage = new FixRawMessage(sourceFile, messageStartOffset, normalized, currentTimestamp, currentDirection);
                resetMessageCapture();
            }
        }

        private void updateStartMatcher(byte value) {
            if (value == FIX_START_MARKER[startMatchLength]) {
                startMatchLength++;
            } else {
                startMatchLength = (value == FIX_START_MARKER[0]) ? 1 : 0;
            }
        }

        private void startMessageCapture() {
            byte[] context = lineContext.snapshot();
            int prefixLength = Math.max(0, context.length - FIX_START_MARKER.length);
            String prefix = new String(context, 0, prefixLength, StandardCharsets.ISO_8859_1);
            currentTimestamp = extractTimestamp(prefix);
            currentDirection = extractDirection(prefix);

            inMessage = true;
            messageStartOffset = absoluteOffset - FIX_START_MARKER.length + 1;
            startMatchLength = 0;
            checksumSearchIndex = 0;
            messageBuffer.clear();
            messageBuffer.append(FIX_START_MARKER);
            lineContext.clear();
        }

        private void resetMessageCapture() {
            inMessage = false;
            checksumSearchIndex = 0;
            messageBuffer.clear();
            currentTimestamp = Optional.empty();
            currentDirection = Optional.empty();
        }

        private void finishAtEndOfFile() {
            if (!inMessage) {
                return;
            }
            TerminatorScan terminator = findChecksumTerminator(
                messageBuffer.array(),
                messageBuffer.size(),
                checksumSearchIndex,
                true,
                config
            );
            if (terminator.found()) {
                byte[] normalized = normalizeDelimiters(messageBuffer.array(), terminator.endExclusive(), config);
                nextMessage = new FixRawMessage(sourceFile, messageStartOffset, normalized, currentTimestamp, currentDirection);
            }
            resetMessageCapture();
        }

        private void closeQuietly() {
            if (closed) {
                return;
            }
            closed = true;
            try {
                channel.close();
            } catch (IOException ignored) {
                // Ignore close errors while already propagating scanner failure.
            }
        }
    }
}
