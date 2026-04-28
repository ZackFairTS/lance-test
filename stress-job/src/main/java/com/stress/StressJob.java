package com.stress;

import org.apache.flink.connector.lance.LanceSink;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.concurrent.ThreadLocalRandom;

public class StressJob {
    public static void main(String[] args) throws Exception {
        String s3Path  = System.getenv().getOrDefault("S3_PATH",  "s3://lance-benchmark-<ACCOUNT_ID>-ap-northeast-1/stress-test/default/dataset");
        long   ratePerSecTotal = Long.parseLong(System.getenv().getOrDefault("RATE_TOTAL", "10000"));
        int    parallelism     = Integer.parseInt(System.getenv().getOrDefault("PARALLELISM", "4"));
        int    batchSize       = Integer.parseInt(System.getenv().getOrDefault("BATCH_SIZE", "1024"));
        long   runSeconds      = Long.parseLong(System.getenv().getOrDefault("RUN_SECONDS", "1800"));

        System.out.println("=== StressJob config ===");
        System.out.println("S3_PATH: " + s3Path);
        System.out.println("RATE_TOTAL: " + ratePerSecTotal);
        System.out.println("PARALLELISM: " + parallelism);
        System.out.println("BATCH_SIZE: " + batchSize);
        System.out.println("RUN_SECONDS: " + runSeconds);

        long maxRecords = ratePerSecTotal * runSeconds;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cc = env.getCheckpointConfig();
        cc.setCheckpointTimeout(60_000);
        cc.setMinPauseBetweenCheckpoints(2_000);
        cc.setTolerableCheckpointFailureNumber(0);

        GeneratorSource src = new GeneratorSource(ratePerSecTotal, runSeconds);
        DataStream<RowData> stream = env.addSource(src).name("datagen").setParallelism(parallelism)
                                        .returns(org.apache.flink.api.common.typeinfo.Types.GENERIC(RowData.class));

        RowType rowType = RowType.of(
                new BigIntType(false),
                new BigIntType(false),
                new VarCharType(200)
        );

        LanceSink sink = LanceSink.builder()
                .path(s3Path)
                .rowType(rowType)
                .batchSize(batchSize)
                .writeMode(LanceOptions.WriteMode.APPEND)
                .build();

        stream.addSink(sink).name("LanceSink").setParallelism(parallelism);

        env.execute("lance-stress-" + System.currentTimeMillis());
    }

    public static class GeneratorSource extends RichParallelSourceFunction<RowData> {
        private static final long serialVersionUID = 1L;
        private final long totalRatePerSec;
        private final long runSeconds;
        private volatile boolean running = true;

        public GeneratorSource(long totalRatePerSec, long runSeconds) {
            this.totalRatePerSec = totalRatePerSec;
            this.runSeconds = runSeconds;
        }

        @Override
        public void run(SourceContext<RowData> ctx) throws Exception {
            int subtask = getRuntimeContext().getIndexOfThisSubtask();
            int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
            long perSubtaskRate = Math.max(1L, totalRatePerSec / parallelism);
            long intervalNanos = 1_000_000_000L / perSubtaskRate;
            long seqBase = (long) subtask * 10_000_000_000L;
            long seq = 0;
            long endTime = System.currentTimeMillis() + runSeconds * 1000L;
            long nextEmit = System.nanoTime();

            while (running && System.currentTimeMillis() < endTime) {
                long now = System.nanoTime();
                if (now < nextEmit) {
                    long sleepMs = (nextEmit - now) / 1_000_000L;
                    if (sleepMs > 0) Thread.sleep(sleepMs);
                    continue;
                }
                GenericRowData r = new GenericRowData(3);
                r.setField(0, seqBase + seq);
                r.setField(1, System.currentTimeMillis());
                byte[] buf = new byte[40];
                ThreadLocalRandom.current().nextBytes(buf);
                StringBuilder sb = new StringBuilder(80);
                for (byte b : buf) { sb.append(String.format("%02x", b)); }
                r.setField(2, StringData.fromString(sb.toString()));
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(r);
                }
                seq++;
                nextEmit += intervalNanos;
            }
        }

        @Override
        public void cancel() { running = false; }
    }
}
