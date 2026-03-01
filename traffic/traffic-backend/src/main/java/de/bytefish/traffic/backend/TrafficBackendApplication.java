package de.bytefish.traffic.backend;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.bytefish.traffic.shared.TelemetryEvent;
import io.nats.client.*;
import io.nats.client.api.StreamConfiguration;
import io.synadia.flink.message.SourceConverter;
import io.synadia.flink.source.NatsSource;
import io.synadia.flink.source.NatsSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
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
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import jakarta.annotation.PostConstruct;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@SpringBootApplication
public class TrafficBackendApplication {

    public static HistoricalSpeedStore HISTORICAL_STORE_INSTANCE;
    public static TrafficStateStore STATE_STORE_INSTANCE;

    public TrafficBackendApplication(HistoricalSpeedStore historicalStore, TrafficStateStore stateStore) {
        HISTORICAL_STORE_INSTANCE = historicalStore;
        STATE_STORE_INSTANCE = stateStore;
    }

    public static void main(String[] args) {
        SpringApplication.run(TrafficBackendApplication.class, args);
    }

    public record TrafficFlowResult(String h3_12, String directionBucket, double avgSpeed, double baselineSpeed, double congestionIndex, int vehicleCount, String trafficState) implements Serializable {}

    public record RawAggregation(String h3, String direction, double avgSpeed, int count) implements Serializable {}

    public static class TelemetrySourceConverter implements SourceConverter<TelemetryEvent> {
        private final ObjectMapper mapper = new ObjectMapper();
        public TelemetrySourceConverter() {}

        @Override
        public TelemetryEvent convert(Message m) {
            try { return mapper.readValue(m.getData(), TelemetryEvent.class); }
            catch (IOException e) { throw new RuntimeException(e); }
        }

        @Override
        public TypeInformation<TelemetryEvent> getProducedType() { return TypeInformation.of(TelemetryEvent.class); }
    }

    public static class DirectionHelper {
        public static String getBucket(double heading) {
            if (heading >= 315 || heading < 45) return "N";
            if (heading >= 45 && heading < 135) return "E";
            if (heading >= 135 && heading < 225) return "S";
            return "W";
        }
    }

    public static class FlinkProcessingEvent implements Serializable {
        public String h3; public String directionBucket; public double speed;
        public FlinkProcessingEvent() {}
        public FlinkProcessingEvent(String h, String d, double s) { this.h3=h; this.directionBucket=d; this.speed=s; }
    }

    public static class TrafficAccumulator implements Serializable {
        public String h3; public String directionBucket; public double sumSpeed = 0; public int count = 0;
        public TrafficAccumulator() {}
    }

    public static class TrafficAggregator implements AggregateFunction<FlinkProcessingEvent, TrafficAccumulator, RawAggregation> {
        @Override public TrafficAccumulator createAccumulator() { return new TrafficAccumulator(); }
        @Override public TrafficAccumulator add(FlinkProcessingEvent v, TrafficAccumulator a) {
            a.h3 = v.h3; a.directionBucket = v.directionBucket; a.sumSpeed += v.speed; a.count += 1; return a;
        }
        @Override public RawAggregation getResult(TrafficAccumulator a) { return new RawAggregation(a.h3, a.directionBucket, a.sumSpeed / a.count, a.count); }
        @Override public TrafficAccumulator merge(TrafficAccumulator a, TrafficAccumulator b) { return null; }
    }

    public static class CongestionEnricher extends RichMapFunction<RawAggregation, TrafficFlowResult> {
        private transient HistoricalSpeedStore store;
        public CongestionEnricher() {}

        @Override
        public void open(OpenContext context) throws Exception {
            this.store = HISTORICAL_STORE_INSTANCE;
        }


        @Override
        public TrafficFlowResult map(RawAggregation r) {
            double b = store.getBaselineSpeed(r.h3(), r.direction());
            double ci = b > 0 ? r.avgSpeed() / b : 1.0;
            String s = b <= 0 ? "LEARNING" : (ci < 0.4 ? "JAM" : (ci < 0.75 ? "SLOW" : "FREE"));
            return new TrafficFlowResult(r.h3(), r.direction(), r.avgSpeed(), b, ci, r.count(), s);
        }
    }

    public static class LocalStateSink implements Sink<TrafficFlowResult> {
        public LocalStateSink() {}
        @Override public SinkWriter<TrafficFlowResult> createWriter(WriterInitContext c) {
            return new SinkWriter<>() {
                @Override public void write(TrafficFlowResult v, Context ctx) { STATE_STORE_INSTANCE.updateState(v); }
                @Override public void flush(boolean e) {}
                @Override public void close() {}
            };
        }
    }
}

@Configuration
class NatsConfig {
    @Bean
    public Connection natsConnection() throws Exception {
        Options options = new Options.Builder()
                .server("nats://localhost:4222")
                .userInfo("user", "password")
                .sslContextFactory(new InsecureSSLFactory())
                .build();
        Connection nc = Nats.connect(options);
        JetStreamManagement jsm = nc.jetStreamManagement();
        try { jsm.getStreamInfo("TELEMETRY"); }
        catch (JetStreamApiException e) {
            StreamConfiguration sc = StreamConfiguration.builder().name("TELEMETRY").subjects("telemetry.>").build();
            jsm.addStream(sc);
        }
        return nc;
    }
}

@Service
class HistoricalSpeedStore {
    private final Map<String, List<Double>> speedObservations = new ConcurrentHashMap<>();
    public void observeAndLearnSpeed(String h3_12, double heading, double speed) {
        String key = h3_12 + "_" + TrafficBackendApplication.DirectionHelper.getBucket(heading);
        speedObservations.computeIfAbsent(key, k -> new CopyOnWriteArrayList<>()).add(speed);
    }
    public double getBaselineSpeed(String h3_12, String directionBucket) {
        List<Double> observations = speedObservations.get(h3_12 + "_" + directionBucket);
        if (observations == null || observations.size() < 30) return -1.0;
        List<Double> sorted = new java.util.ArrayList<>(observations);
        java.util.Collections.sort(sorted);
        return sorted.get((int) Math.ceil(0.85 * sorted.size()) - 1);
    }
}

@Service
class TrafficStateStore {
    private final Map<String, TrafficBackendApplication.TrafficFlowResult> currentState = new ConcurrentHashMap<>();
    public void updateState(TrafficBackendApplication.TrafficFlowResult result) {
        currentState.put(result.h3_12() + "_" + result.directionBucket(), result);
    }
    public TrafficBackendApplication.TrafficFlowResult getState(String h3_12, double heading) {
        String bucket = TrafficBackendApplication.DirectionHelper.getBucket(heading);
        return currentState.getOrDefault(h3_12 + "_" + bucket,
                new TrafficBackendApplication.TrafficFlowResult(h3_12, bucket, -1, -1, -1, 0, "UNKNOWN"));
    }
}

@RestController
@RequestMapping("/api/traffic")
class TrafficController {
    private final JetStream js;
    private final ObjectMapper mapper;
    private final TrafficStateStore stateStore;
    private final HistoricalSpeedStore historicalStore;

    public TrafficController(Connection nc, TrafficStateStore stateStore, HistoricalSpeedStore historicalStore) throws IOException {
        this.js = nc.jetStream();
        this.stateStore = stateStore;
        this.historicalStore = historicalStore;
        this.mapper = new ObjectMapper();
    }

    @PostMapping(value = "/telemetry", consumes = MediaType.APPLICATION_JSON_VALUE)
    public Mono<TrafficBackendApplication.TrafficFlowResult> receiveTelemetry(@RequestBody byte[] rawPayload) {
        return Mono.fromCallable(() -> mapper.readValue(rawPayload, TelemetryEvent.class))
                .flatMap(event -> {
                    historicalStore.observeAndLearnSpeed(event.h3_12(), event.heading(), event.speed());
                    return Mono.fromFuture(js.publishAsync("telemetry.v1", rawPayload))
                            .map(ack -> stateStore.getState(event.h3_12(), event.heading()));
                });
    }
}

@Service
class FlinkJobManager {
    @PostConstruct
    public void startFlinkJob() {
        new Thread(() -> {
            try {
                Thread.sleep(2000);
                StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
                Properties natsProps = new Properties();
                natsProps.setProperty(Options.PROP_URL, "nats://localhost:4222");
                natsProps.setProperty(Options.PROP_USERNAME, "user");
                natsProps.setProperty(Options.PROP_PASSWORD, "password");
                natsProps.setProperty(Options.PROP_SECURE, "true");
                natsProps.setProperty(Options.PROP_SSL_CONTEXT_FACTORY_CLASS, InsecureSSLFactory.class.getName());

                NatsSource<TelemetryEvent> natsSource = new NatsSourceBuilder<TelemetryEvent>()
                        .connectionProperties(natsProps)
                        .subjects("telemetry.>")
                        .sourceConverter(new TrafficBackendApplication.TelemetrySourceConverter())
                        .build();

                env.fromSource(natsSource, WatermarkStrategy.noWatermarks(), "NATS Source")
                        .map(e -> new TrafficBackendApplication.FlinkProcessingEvent(e.h3_12(), TrafficBackendApplication.DirectionHelper.getBucket(e.heading()), e.speed()))
                        .keyBy(e -> e.h3 + "_" + e.directionBucket)
                        .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
                        .aggregate(new TrafficBackendApplication.TrafficAggregator())
                        .map(new TrafficBackendApplication.CongestionEnricher())
                        .sinkTo(new TrafficBackendApplication.LocalStateSink());

                env.execute("Traffic Backend");
            } catch (Exception e) { e.printStackTrace(); }
        }).start();
    }
}