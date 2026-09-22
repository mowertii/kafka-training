package ru.otus.kafka.hw6.health;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import ru.otus.kafka.common.EnvUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Простой HTTP-сервер для health-checks.
 * 
 * Endpoints:
 * - GET /health — общий статус (всегда 200)
 * - GET /ready  — готовность (проверка Kafka; 200 или 503)
 * - GET /live   — живость процесса (всегда 200)
 */
public class HealthServer {
    private final int port;
    private final String bootstrapServers;
    private HttpServer server;

    public HealthServer(int port, String bootstrapServers) {
        this.port = port;
        this.bootstrapServers = bootstrapServers;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", this::handleHealth);
        server.createContext("/ready", this::handleReady);
        server.createContext("/live", this::handleLive);
        server.setExecutor(Executors.newFixedThreadPool(2));
        server.start();
        System.out.println("Health server started on port " + port);
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        respond(exchange, 200, "{\"status\":\"UP\"}");
    }

    private void handleLive(HttpExchange exchange) throws IOException {
        respond(exchange, 200, "{\"status\":\"UP\",\"check\":\"liveness\"}");
    }

    private void handleReady(HttpExchange exchange) throws IOException {
        try {
            // Проверяем доступность Kafka
            try (AdminClient admin = AdminClient.create(Map.of(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
                admin.listTopics().names().get();
            }
            respond(exchange, 200, "{\"status\":\"UP\",\"kafka\":\"available\"}");
        } catch (Exception e) {
            respond(exchange, 503, "{\"status\":\"DOWN\",\"kafka\":\"unavailable\",\"error\":\"" 
                    + e.getMessage() + "\"}");
        }
    }

    private void respond(HttpExchange exchange, int status, String body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, body.getBytes().length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body.getBytes());
        }
    }
}