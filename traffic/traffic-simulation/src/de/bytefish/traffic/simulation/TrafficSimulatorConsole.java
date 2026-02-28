package com.verkehr.simulator;

import com.uber.h3core.H3Core;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class TrafficSimulatorConsole {

    private static final String BACKEND_URL = "http://localhost:8080/api/traffic/telemetry";
    
    private static final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
            
    private static final Random random = new Random();

    // A single high-resolution point on the route
    record RoutePoint(String h3, double lat, double lon, double distanceMetersFromStart) {}

    public static void main(String[] args) throws Exception {
        System.out.println("=== Traffic Simulator: Continuous A1 Route ===");

        // Initialize the H3 Core Library
        H3Core h3 = H3Core.newInstance();

        // Start and end of the A1 route (Ladbergen -> Münster North)
        double startLat = 52.175;
        double startLon = 7.825;
        double endLat = 52.015;
        double endLon = 7.615;
        double headingMuenster = 220.0;

        System.out.println("-> Generating high-resolution route...");
        List<RoutePoint> route = generateContinuousRoute(h3, startLat, startLon, endLat, endLon, 12);
        
        System.out.println("-> Route generated! Length: " + Math.round(route.get(route.size()-1).distanceMetersFromStart() / 1000.0) + " km");
        System.out.println("-> Number of unique 9-meter H3 cells on this route: " + route.size());

        int historicalCars = 35; // Cars per cell for the learning phase
        int liveCars = 5;        // Cars per cell for the live phase (reduced since we now have hundreds of cells)

        // -----------------------------------------------------------------
        // PHASE 1: Learning Phase (The entire route learns "Free Flow")
        // -----------------------------------------------------------------
        System.out.println("\n[Phase 1] Collecting historical data for all " + route.size() + " cells (130 km/h baseline)...");
        
        for (int cellIndex = 0; cellIndex < route.size(); cellIndex++) {
            RoutePoint pt = route.get(cellIndex);
            
            // Progress bar in the console to show progress
            if (cellIndex % 50 == 0) System.out.print(".");

            for (int i = 0; i < historicalCars; i++) {
                double speed = 130.0 + (random.nextGaussian() * 10.0);
                speed = Math.max(80.0, Math.min(speed, 220.0));
                sendTelemetry("Hist_Car_" + i, pt.h3(), pt.lat(), pt.lon(), speed, headingMuenster);
            }
        }

        System.out.println("\n-> Historical route learned. Waiting 6 seconds for the Flink window to close...");
        Thread.sleep(6000);

        // -----------------------------------------------------------------
        // PHASE 2: Live Scenario (The traffic jam between km 8 and km 13)
        // -----------------------------------------------------------------
        System.out.println("\n[Phase 2] Simulating live traffic: Massive traffic jam between km 8 and km 13!");

        int jamStartMeters = 8000;
        int jamEndMeters = 13000;

        for (int cellIndex = 0; cellIndex < route.size(); cellIndex++) {
            RoutePoint pt = route.get(cellIndex);
            
            // Determine speed based on position (distance from start)
            double targetSpeed;
            boolean inJam = pt.distanceMetersFromStart() >= jamStartMeters && pt.distanceMetersFromStart() <= jamEndMeters;
            
            if (inJam) {
                targetSpeed = 25.0; // Traffic jam!
            } else if (pt.distanceMetersFromStart() > jamEndMeters && pt.distanceMetersFromStart() < jamEndMeters + 2000) {
                targetSpeed = 80.0; // Sluggish right after the traffic jam (dissolving)
            } else {
                targetSpeed = 125.0; // Free flow
            }

            for (int i = 0; i < liveCars; i++) {
                double stdDeviation = targetSpeed < 50 ? 3.0 : 8.0;
                double speed = targetSpeed + (random.nextGaussian() * stdDeviation);
                speed = Math.max(0.0, speed); 
                
                String response = sendTelemetry("Live_Car_" + i, pt.h3(), pt.lat(), pt.lon(), speed, headingMuenster);
                
                // We only log every 100th cell or the exact traffic jam transition to avoid spam
                if (i == liveCars - 1) {
                    if (cellIndex % 100 == 0 || Math.abs(pt.distanceMetersFromStart() - jamStartMeters) < 20 || Math.abs(pt.distanceMetersFromStart() - jamEndMeters) < 20) {
                        System.out.printf("   [km %4.1f] Target Speed: %3.0f km/h -> API: %s%n", (pt.distanceMetersFromStart() / 1000.0), targetSpeed, response);
                    }
                }
            }
            // Short sleep so Flink is not overloaded (we are firing an extreme amount of events here)
            Thread.sleep(10);
        }

        System.out.println("\n=== Simulation finished. The Flink backend should have detected a beautiful traffic jam shockwave by now! ===");
    }

    /**
     * Calculates a continuous line between two GPS points and determines all 
     * H3 cells that lie on this line.
     */
    private static List<RoutePoint> generateContinuousRoute(H3Core h3, double lat1, double lon1, double lat2, double lon2, int resolution) {
        List<RoutePoint> route = new ArrayList<>();
        Set<String> seenH3Cells = new LinkedHashSet<>(); // Keeps the order, but filters duplicates

        double totalDistance = haversineDistance(lat1, lon1, lat2, lon2);
        int stepMeters = 5; // We check every 5 meters to guarantee no 9-meter cell is skipped
        int steps = (int) (totalDistance / stepMeters);

        for (int i = 0; i <= steps; i++) {
            double fraction = (double) i / steps;
            // Simple linear interpolation (sufficient for distances < 50km)
            double currentLat = lat1 + (lat2 - lat1) * fraction;
            double currentLon = lon1 + (lon2 - lon1) * fraction;
            double currentDist = fraction * totalDistance;

            String cellAddress = h3.latLngToCellAddress(currentLat, currentLon, resolution);

            // Only add if we don't already have this cell on the route
            if (seenH3Cells.add(cellAddress)) {
                route.add(new RoutePoint(cellAddress, currentLat, currentLon, currentDist));
            }
        }

        return route;
    }

    /**
     * Calculates the exact distance in meters between two GPS coordinates.
     */
    private static double haversineDistance(double lat1, double lon1, double lat2, double lon2) {
        final int R = 6371000; // Earth radius in meters
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }

    private static String sendTelemetry(String vid, String h3, double lat, double lon, double speed, double heading) {
        speed = Math.round(speed * 10.0) / 10.0;
        String jsonPayload = String.format(
                "{\"vehicleId\":\"%s\", \"lat\":%s, \"lon\":%s, \"h3_12\":\"%s\", \"speed\":%s, \"heading\":%s}",
                vid, lat, lon, h3, speed, heading
        );

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(BACKEND_URL))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return response.body();
        } catch (Exception e) {
            return "ERROR";
        }
    }
}