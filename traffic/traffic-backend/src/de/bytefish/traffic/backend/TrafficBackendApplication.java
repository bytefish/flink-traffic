package de.bytefish.traffic.backend;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.*;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StorageType;
import io.synadia.flink.message.SourceConverter;
import io.synadia.flink.source.NatsSource;
import io.synadia.flink.source.NatsSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@SpringBootApplication
public class TrafficBackendApplication {
    public static void main(String[] args) {
        SpringApplication.run(TrafficBackendApplication.class, args);
    }
}

// ============================================================================
// DATA MODELS
// ============================================================================
record TelemetryEvent(String vehicleId, double lat, double lon, String h3_12, double speed, double heading) {}
record TrafficFlowResult(String h3_12, String directionBucket, double avgSpeed, double baselineSpeed, double congestionIndex, int vehicleCount, String trafficState) {}
record RawAggregation(String h3, String direction, double avgSpeed, int count) {}

// ============================================================================
// NATS CONFIGURATION
// ============================================================================
@Configuration
class NatsConfig {
    @Bean
    public Connection natsConnection() throws Exception {
        Connection nc = Nats.connect("nats://localhost:4222");
        JetStreamManagement jsm = nc.jetStreamManagement();
        try {
            jsm.getStreamInfo("TELEMETRY");
        } catch (JetStreamApiException e) {
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name("TELEMETRY")
                    .subjects("telemetry.>")
                    .storageType(StorageType.Memory)
                    .build();
            jsm.addStream(streamConfig);
        }
        return nc;
    }
}

// ============================================================================
// HISTORICAL STORE
// ============================================================================
@Service
class HistoricalSpeedStore {
    private final Map<String, List<Double>> speedObservations = new ConcurrentHashMap<>();
    private static final int MIN_OBSERVATIONS = 30;

    public void observeAndLearnSpeed(String h3_12, double heading, double speed) {
        String key = h3_12 + "_" + DirectionHelper.getBucket(heading);
        speedObservations.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(speed);
    }

    public double getBaselineSpeed(String h3_12, String directionBucket) {
        String key = h3_12 + "_" + directionBucket;
        List<Double> observations = speedObservations.get(key);

        if (observations == null || observations.size() < MIN_OBSERVATIONS) {
            return -1.0;
        }

        List<Double> sorted = new java.util.ArrayList<>(observations);
        Collections.sort(sorted);
        int index85 = (int) Math.ceil(0.85 * sorted.size()) - 1;
        return sorted.get(Math.max(0, index85));
    }

    public int getObservationCount(String h3_12, String directionBucket) {
        List<Double> obs = speedObservations.get(h3_12 + "_" + directionBucket);
        return obs != null ? obs.size() : 0;
    }
}

@Service
class TrafficStateStore {
    private final Map<String, TrafficFlowResult> currentState = new ConcurrentHashMap<>();
    public void updateState(TrafficFlowResult result) {
        currentState.put(result.h3_12() + "_" + result.directionBucket(), result);
    }
    public TrafficFlowResult getState(String h3_12, double heading) {
        String bucket = DirectionHelper.getBucket(heading);
        return currentState.getOrDefault(h3_12 + "_" + bucket,
                new TrafficFlowResult(h3_12, bucket, -1, -1, -1, 0, "UNKNOWN"));
    }
}

// ============================================================================
// HIGH-PERFORMANCE REACTIVE REST API (WEBFLUX)
// ============================================================================
@RestController
@RequestMapping("/api/traffic")
class TrafficController {
    private final JetStream js;
    private final ObjectMapper mapper;
    private final TrafficStateStore stateStore;
    private final HistoricalSpeedStore historicalStore;

    public TrafficController(Connection nc, TrafficStateStore stateStore, HistoricalSpeedStore historicalStore) throws IOException {
        this.js = nc.jetStream(); // Cache JetStream instance for performance
        this.stateStore = stateStore;
        this.historicalStore = historicalStore;
        this.mapper = new ObjectMapper();
    }

    /**
     * Fully reactive, non-blocking endpoint with zero-serialization overhead for NATS.
     */
    @PostMapping("/telemetry")
    public Mono<TrafficFlowResult> receiveTelemetry(@RequestBody byte[] rawPayload) {
        return Mono.fromCallable(() -> {
            // 1. FAST DESERIALIZATION: Parse the raw bytes directly into the record
            return mapper.readValue(rawPayload, TelemetryEvent.class);
        }).flatMap(event -> {
            // 2. Update memory store
            historicalStore.observeAndLearnSpeed(event.h3_12(), event.heading(), event.speed());

            // 3. ZERO-COPY ROUTING: Push the *exact original bytes* to NATS.
            // publishAsync returns a CompletableFuture, which perfectly integrates into WebFlux Mono.
            return Mono.fromFuture(js.publishAsync("telemetry.v1", rawPayload))
                    .map(ack -> stateStore.getState(event.h3_12(), event.heading()));
        });
    }
}

// ============================================================================
// APACHE FLINK 2.0 JOB & LOGIC
// ============================================================================
@Service
class FlinkJobManager {
    private final TrafficStateStore stateStore;
    private final HistoricalSpeedStore historicalStore;

    public FlinkJobManager(TrafficStateStore stateStore, HistoricalSpeedStore historicalStore) {
        this.stateStore = stateStore;
        this.historicalStore = historicalStore;
    }

    @PostConstruct
    public void startFlinkJob() {
        new Thread(() -> {
            try {
                StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

                Properties natsProps = new Properties();
                natsProps.setProperty(Options.PROP_URL, "nats://localhost:4222");

                SourceConverter<TelemetryEvent> converter = new SourceConverter<TelemetryEvent>() {
                    private final ObjectMapper mapper = new ObjectMapper();
                    @Override
                    public TelemetryEvent convert(Message message) {
                        try {
                            return mapper.readValue(message.getData(), TelemetryEvent.class);
                        } catch (IOException e) {
                            throw new RuntimeException("Error deserializing TelemetryEvent", e);
                        }
                    }
                    @Override
                    public TypeInformation<TelemetryEvent> getProducedType() {
                        return TypeInformation.of(TelemetryEvent.class);
                    }
                };

                NatsSource<TelemetryEvent> natsSource = new NatsSourceBuilder<TelemetryEvent>()
                        .connectionProperties(natsProps)
                        .subjects("telemetry.>")
                        .sourceConverter(converter)
                        .build();

                env.fromSource(natsSource, WatermarkStrategy.noWatermarks(), "Synadia NATS Source")
                        .map(event -> new FlinkProcessingEvent(event.h3_12(), DirectionHelper.getBucket(event.heading()), event.speed()))
                        .keyBy(event -> event.h3 + "_" + event.directionBucket)
                        .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                        .aggregate(new TrafficAggregator())
                        .map(new CongestionEnricher(historicalStore))
                        .sinkTo(new LocalStateSink(stateStore, historicalStore));

                env.execute("Smart H3 Traffic Detection (Reactive Backend)");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }
}

class DirectionHelper {
    public static String getBucket(double heading) {
        if (heading >= 315 || heading < 45) return "N";
        if (heading >= 45 && heading < 135) return "E";
        if (heading >= 135 && heading < 225) return "S";
        return "W";
    }
}

class FlinkProcessingEvent {
    public String h3; public String directionBucket; public double speed;
    public FlinkProcessingEvent(String h3, String d, double s) { this.h3 = h3; this.directionBucket = d; this.speed = s; }
}

class TrafficAccumulator {
    public String h3; public String directionBucket;
    public double sumSpeed = 0; public int count = 0;
}

class TrafficAggregator implements AggregateFunction<FlinkProcessingEvent, TrafficAccumulator, RawAggregation> {
    @Override public TrafficAccumulator createAccumulator() { return new TrafficAccumulator(); }
    @Override public TrafficAccumulator add(FlinkProcessingEvent value, TrafficAccumulator acc) {
        acc.h3 = value.h3; acc.directionBucket = value.directionBucket;
        acc.sumSpeed += value.speed; acc.count += 1;
        return acc;
    }
    @Override public RawAggregation getResult(TrafficAccumulator acc) {
        return new RawAggregation(acc.h3, acc.directionBucket, acc.sumSpeed / acc.count, acc.count);
    }
    @Override public TrafficAccumulator merge(TrafficAccumulator a, TrafficAccumulator b) { return null; }
}

class CongestionEnricher extends RichMapFunction<RawAggregation, TrafficFlowResult> {
    private final HistoricalSpeedStore historicalStore;
    public CongestionEnricher(HistoricalSpeedStore historicalStore) { this.historicalStore = historicalStore; }

    @Override
    public TrafficFlowResult map(RawAggregation raw) {
        double baseline = historicalStore.getBaselineSpeed(raw.h3(), raw.direction());
        String state = "FREE_FLOW";
        double congestionIndex = 1.0;

        if (baseline > 0 && raw.count() >= 2) {
            congestionIndex = raw.avgSpeed() / baseline;
            if (congestionIndex < 0.40) state = "HEAVY_JAM";
            else if (congestionIndex < 0.75) state = "SLOW_TRAFFIC";
        } else if (baseline <= 0) {
            state = "GATHERING_DATA";
        } else if (raw.count() < 2) {
            state = "UNKNOWN (Not enough live data)";
        }

        return new TrafficFlowResult(raw.h3(), raw.direction(), Math.round(raw.avgSpeed() * 10.0)/10.0, Math.round(baseline * 10.0)/10.0, Math.round(congestionIndex * 100.0)/100.0, raw.count(), state);
    }
}

// ============================================================================
// FLINK 2.0 SINK (FLIP-191 API)
// ============================================================================
class LocalStateSink implements Sink<TrafficFlowResult> {
    private final TrafficStateStore store;
    private final HistoricalSpeedStore histStore;

    public LocalStateSink(TrafficStateStore store, HistoricalSpeedStore histStore) {
        this.store = store; this.histStore = histStore;
    }

    @Override
    public SinkWriter<TrafficFlowResult> createWriter(WriterInitContext context) throws IOException {
        return new SinkWriter<>() {
            @Override
            public void write(TrafficFlowResult value, Context ctx) {
                int histCount = histStore.getObservationCount(value.h3_12(), value.directionBucket());
                System.out.printf("[FLINK 2.0 SINK] H3: %s | Ø-Live: %5.1f km/h | Baseline: %5.1f km/h (from %d obs) | Index: %5.2f | Status: %s%n",
                        value.h3_12(), value.avgSpeed(), value.baselineSpeed(), histCount, value.congestionIndex(), value.trafficState());
                store.updateState(value);
            }
            @Override public void flush(boolean endOfInput) {}
            @Override public void close() {}
        };
    }
}