package com.vbforge.kafkalaglab.diagnose;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DiagnoseApp {

    private static final Pattern LAG_LINE = Pattern.compile(
            "^\\S+\\s+\\S+\\s+(\\d+)\\s+(\\d+)\\s+(\\d+)\\s+(-?\\d+)\\s+\\S");

    public static void main(String[] args) throws Exception {
        String group = envOr("GROUP_ID", "lag-lab-group");
        String kafkaContainer = envOr("KAFKA_CONTAINER", "kafka-lag-lab-kafka");
        String bootstrap = envOr("BOOTSTRAP_SERVER", "localhost:19092");
        double intervalSeconds = Double.parseDouble(envOr("INTERVAL_SECONDS", "3"));
        int samples = Integer.parseInt(envOr("SAMPLES", "5"));
        String rebalanceLogPath = System.getenv("REBALANCE_LOG");

        long rebalanceLineCursor = 0;

        System.out.printf("kafka-lag-lab :: auto-diagnosis :: group=%s window=%d samples @ %.1fs%n%n",
                group, samples, intervalSeconds);

        while (true) {
            Map<Integer, List<Long>> windowHistory = new HashMap<>();
            int rebalanceTotal = 0;

            for (int i = 0; i < samples; i++) {
                Map<Integer, Long> lags = fetchLag(kafkaContainer, bootstrap, group);
                for (Map.Entry<Integer, Long> e : lags.entrySet()) {
                    windowHistory.computeIfAbsent(e.getKey(), k -> new ArrayList<>()).add(e.getValue());
                }

                if (rebalanceLogPath != null) {
                    RebalanceCount rc = countRebalances(rebalanceLogPath, rebalanceLineCursor);
                    rebalanceTotal += rc.count;
                    rebalanceLineCursor = rc.newCursor;
                }

                if (i < samples - 1) {
                    Thread.sleep((long) (intervalSeconds * 1000));
                }
            }

            if (windowHistory.isEmpty()) {
                System.out.println("No consumer group data yet — is the consumer running?");
            } else {
                System.out.println("=== window @ " + java.time.LocalTime.now().withNano(0) + " ===");
                System.out.println(diagnose(windowHistory, rebalanceTotal));
                System.out.println();
            }
        }
    }

    private static Map<Integer, Long> fetchLag(String kafkaContainer, String bootstrap, String group) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(
                "docker", "exec", kafkaContainer,
                "kafka-consumer-groups", "--bootstrap-server", bootstrap,
                "--describe", "--group", group
        );
        pb.redirectErrorStream(true);
        Process process = pb.start();

        Map<Integer, Long> lags = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Matcher m = LAG_LINE.matcher(line.trim());
                if (m.find()) {
                    int partition = Integer.parseInt(m.group(1));
                    long lag = Long.parseLong(m.group(4));
                    lags.put(partition, lag);
                }
            }
        }
        process.waitFor();
        return lags;
    }

    private record RebalanceCount(int count, long newCursor) {}

    private static RebalanceCount countRebalances(String logPath, long sinceLine) {
        try {
            List<String> lines = Files.readAllLines(Path.of(logPath), StandardCharsets.UTF_8);
            int count = 0;
            for (int i = (int) sinceLine; i < lines.size(); i++) {
                if (lines.get(i).contains("[REBALANCE] partitions revoked")) {
                    count++;
                }
            }
            return new RebalanceCount(count, lines.size());
        } catch (IOException e) {
            return new RebalanceCount(0, sinceLine);
        }
    }

    private static String diagnose(Map<Integer, List<Long>> history, int rebalanceCount) {
        Map<Integer, Long> latest = new HashMap<>();
        Map<Integer, Long> delta = new HashMap<>();

        for (Map.Entry<Integer, List<Long>> e : history.entrySet()) {
            List<Long> h = e.getValue();
            latest.put(e.getKey(), h.get(h.size() - 1));
            delta.put(e.getKey(), h.get(h.size() - 1) - h.get(0));
        }

        double avgLag = latest.values().stream().mapToLong(Long::longValue).average().orElse(0);
        int maxPartition = latest.entrySet().stream().max(Map.Entry.comparingByValue()).map(Map.Entry::getKey).orElse(-1);
        long maxLag = maxPartition >= 0 ? latest.get(maxPartition) : 0;
        double skewRatio = avgLag > 0 ? maxLag / avgLag : 1.0;

        long growingCount = delta.values().stream().filter(d -> d > 20).count();
        boolean allGrowing = growingCount >= Math.max(1, history.size() - 1);

        List<Long> stepDeltas = new ArrayList<>();
        for (List<Long> h : history.values()) {
            for (int i = 1; i < h.size(); i++) {
                stepDeltas.add(h.get(i) - h.get(i - 1));
            }
        }
        boolean erratic = false;
        if (stepDeltas.size() >= 3) {
            double meanStep = stepDeltas.stream().mapToLong(Long::longValue).average().orElse(0);
            double variance = stepDeltas.stream().mapToDouble(d -> Math.pow(d - meanStep, 2)).average().orElse(0);
            double stdevStep = Math.sqrt(variance);
            erratic = stdevStep > Math.abs(meanStep) * 2.5 && stdevStep > 30;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("avg lag=%.0f  max lag=%d (partition %d)  skew ratio=%.1fx%n",
                avgLag, maxLag, maxPartition, skewRatio));
        sb.append("rebalance events in window: ").append(rebalanceCount).append("\n\n");

        if (rebalanceCount >= 2) {
            sb.append("DIAGNOSIS: Rebalance thrashing\n");
            sb.append("  reason: ").append(rebalanceCount).append(" rebalance events observed in this window — the group\n");
            sb.append("  is losing processing time to repeated partition reassignment.");
        } else if (avgLag < 50 && maxLag < 100) {
            sb.append("DIAGNOSIS: Healthy — no bottleneck detected\n");
            sb.append("  reason: lag is low and not accumulating across the window.");
        } else if (skewRatio >= 3 && growingCount <= Math.max(1, history.size() / 3)) {
            sb.append("DIAGNOSIS: Hot partition (partition ").append(maxPartition).append(")\n");
            sb.append(String.format("  reason: partition %d lag is %.1fx the average while most others are flat.%n", maxPartition, skewRatio));
        } else if (erratic) {
            sb.append("DIAGNOSIS: Retry storm (erratic growth)\n");
            sb.append("  reason: lag growth is bursty and uneven step-to-step.");
        } else if (allGrowing) {
            sb.append("DIAGNOSIS: Slow / synchronous processing bottleneck\n");
            sb.append("  reason: lag is climbing at a similar rate across nearly all partitions.");
        } else {
            sb.append("DIAGNOSIS: Inconclusive — keep watching\n");
            sb.append("  reason: no single pattern dominates yet.");
        }

        return sb.toString();
    }

    private static String envOr(String name, String fallback) {
        String v = System.getenv(name);
        return (v == null || v.isBlank()) ? fallback : v;
    }
}