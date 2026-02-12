package org.example;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.nio.charset.StandardCharsets;

public class CalculateAverage {

    private static final int WORKER_COUNT =
            Math.max(2, Runtime.getRuntime().availableProcessors() / 2);

    private static final int BUF_SIZE = 1 << 20; // 1 MB

    public static void main(String[] args) throws Exception {
        long start = System.nanoTime();

        Path input = Path.of("src/test/resources/measurements.txt");
        Path output = Path.of("measurements.out");

        long fileSize;
        try (FileChannel channel = FileChannel.open(input, StandardOpenOption.READ)) {
            fileSize = channel.size();
        }

        long chunkSize = fileSize / WORKER_COUNT;
        List<Chunk> chunks = new ArrayList<>(WORKER_COUNT);

        long curStart = 0;
        for (int i = 0; i < WORKER_COUNT; i++) {
            long estEnd = (i == WORKER_COUNT - 1) ? fileSize : (curStart + chunkSize);
            chunks.add(new Chunk(curStart, estEnd));
            curStart = estEnd;
        }

        try (FileChannel channel = FileChannel.open(input, StandardOpenOption.READ)) {
            adjustChunksToLineBoundaries(channel, chunks, fileSize);
        }

        ExecutorService executor = Executors.newFixedThreadPool(WORKER_COUNT);
        List<Future<Map<StationKey, StationStats>>> futures = new ArrayList<>(WORKER_COUNT);

        for (Chunk chunk : chunks) {
            futures.add(executor.submit(() -> processChunk(input, chunk)));
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        Map<StationKey, StationStats> statsMap = new HashMap<>(1_000_000, 0.75f);
        for (Future<Map<StationKey, StationStats>> f : futures) {
            Map<StationKey, StationStats> local = f.get();
            for (Map.Entry<StationKey, StationStats> e : local.entrySet()) {
                StationKey stationKey = e.getKey();
                StationStats localStats = e.getValue();
                StationStats global = statsMap.get(stationKey);
                if (global == null) {
                    statsMap.put(stationKey, new StationStats(
                            localStats.getMin(),
                            localStats.getMax(),
                            localStats.getSum(),
                            localStats.getCount()
                    ));
                } else {
                    global.merge(localStats);
                }
            }
        }

        writeResult(statsMap, output);

        long end = System.nanoTime();
        double millis = (end - start) / 1_000_000.0;
        System.out.printf("Processing took %.2f ms%n", millis);
    }

    private record Chunk(long start, long end) { }

    private static void adjustChunksToLineBoundaries(FileChannel channel,
                                                     List<Chunk> chunks,
                                                     long fileSize) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(1);

        for (int i = 0; i < chunks.size(); i++) {
            Chunk c = chunks.get(i);
            long start = c.start;
            long end = c.end;

            if (i > 0) {
                start = moveToNextNewline(channel, start, fileSize, buf);
            }
            if (i < chunks.size() - 1) {
                end = moveToPrevNewline(channel, end, fileSize, buf);
            } else {
                end = fileSize;
            }

            if (start > end) {
                start = end;
            }

            chunks.set(i, new Chunk(start, end));
        }
    }

    private static long moveToNextNewline(FileChannel channel, long pos,
                                          long fileSize, ByteBuffer buf) throws IOException {
        if (pos <= 0) return 0;
        long cur = pos;
        while (cur < fileSize) {
            buf.clear();
            int r = channel.read(buf, cur);
            if (r == -1) break;
            buf.flip();
            byte b = buf.get();
            if (b == '\n') {
                return cur + 1;
            }
            cur++;
        }
        return fileSize;
    }

    private static long moveToPrevNewline(FileChannel channel, long pos,
                                          long fileSize, ByteBuffer buf) throws IOException {
        if (pos >= fileSize) return fileSize;
        long cur = pos - 1;
        while (cur >= 0) {
            buf.clear();
            int r = channel.read(buf, cur);
            if (r == -1) break;
            buf.flip();
            byte b = buf.get();
            if (b == '\n') {
                return cur + 1;
            }
            cur--;
        }
        return 0;
    }

    private static Map<StationKey, StationStats> processChunk(Path input, Chunk chunk) {
        Map<StationKey, StationStats> statsMap = new HashMap<>(256_000, 0.75f);

        try (FileChannel channel = FileChannel.open(input, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(BUF_SIZE);
            byte[] lineBuf = new byte[256];
            int lineLen = 0;

            long position = chunk.start;
            while (position < chunk.end) {
                buffer.clear();
                long bytesToRead = Math.min(BUF_SIZE, chunk.end - position);
                buffer.limit((int) bytesToRead);

                int read = channel.read(buffer, position);
                if (read == -1) {
                    break;
                }
                position += read;
                buffer.flip();

                while (buffer.hasRemaining()) {
                    byte b = buffer.get();
                    if (b == '\n') {
                        if (lineLen > 0) {
                            processLine(lineBuf, lineLen, statsMap);
                            lineLen = 0;
                        }
                    } else if (b != '\r') {
                        if (lineLen == lineBuf.length) {
                            lineBuf = Arrays.copyOf(lineBuf, lineBuf.length * 2);
                        }
                        lineBuf[lineLen++] = b;
                    }
                }
            }

            if (lineLen > 0) {
                processLine(lineBuf, lineLen, statsMap);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return statsMap;
    }

    private static void processLine(byte[] buf, int len, Map<StationKey, StationStats> statsMap) {
        int sep = -1;
        for (int i = 0; i < len; i++) {
            if (buf[i] == ';') {
                sep = i;
                break;
            }
        }
        if (sep < 0) {
            return;
        }

        StationKey key = new StationKey(buf, 0, sep);
        int tempInTenths = parseTemperatureToTenths(buf, sep + 1, len);

        StationStats stats = statsMap.get(key);
        if (stats == null) {
            statsMap.put(key, new StationStats(tempInTenths));
        } else {
            stats.add(tempInTenths);
        }
    }

    private static int parseTemperatureToTenths(byte[] buf, int start, int len) {
        boolean negative = false;
        int i = start;
        if (i < len && buf[i] == '-') {
            negative = true;
            i++;
        }
        int value = 0;
        int digits = 0;
        for (; i < len; i++) {
            byte b = buf[i];
            if (b == '.') {
                continue;
            }
            if (b < '0' || b > '9') {
                break;
            }
            value = value * 10 + (b - '0');
            digits++;
            if (digits == 3) {
                break;
            }
        }
        return negative ? -value : value;
    }

    private static double round(double sum, double count) {
        final var value = (Math.round(sum * 10.0) / 10.0) / count;
        return Math.round(value * 10.0) / 10.0;
    }

    private static double average(StationStats stats) {
        double sum = stats.getSum() / 10.0;
        double count = stats.getCount();
        return round(sum, count);
    }

    private static String formatNumber(double value) {
        return String.format(Locale.US, "%.1f", value);
    }

    private static void writeResult(Map<StationKey, StationStats> statsMap, Path output) throws IOException {
        List<StationKey> stations = new ArrayList<>(statsMap.keySet());

        stations.sort(Comparator.comparing(StationKey::toStringUtf8));

        try (BufferedWriter bw = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            boolean first = true;
            for (StationKey key : stations) {
                StationStats stats = statsMap.get(key);
                double min = stats.getMin() / 10.0;
                double avg = average(stats);
                double max = stats.getMax() / 10.0;

                if (!first) {
                    bw.write(", ");
                } else {
                    first = false;
                }
                String station = key.toStringUtf8();
                bw.write(station);
                bw.write("=");
                bw.write(formatNumber(min));
                bw.write("/");
                bw.write(formatNumber(avg));
                bw.write("/");
                bw.write(formatNumber(max));
            }
        }
    }
}
