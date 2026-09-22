package ru.otus.kafka.hw6.schema;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Регистрация Avro-схем в Schema Registry.
 * 
 * Schema Registry хранит:
 * - Схемы (Avro, JSON Schema, Protobuf)
 * - Версии схем
 * - Совместимость между версиями
 * - Маппинг schema ID → схема
 * 
 * Каждый subject = <topic>-value или <topic>-key
 */
public class SchemaRegistrar {

    private final SchemaRegistryClient client;
    private final String subject;

    public SchemaRegistrar(String schemaRegistryUrl, String subject) {
        this.client = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
        this.subject = subject;
    }

    /**
     * Регистрирует схему из файла.
     * 
     * @param schemaResource путь к .avsc в classpath
     * @return ID зарегистрированной схемы
     */
    public int registerSchema(String schemaResource) throws IOException, RestClientException {
        Schema schema = loadSchema(schemaResource);
        
        // Регистрируем схему в Schema Registry
        int schemaId = client.register(subject, schema);
        
        System.out.printf("✅ Схема зарегистрирована: subject=%s, schemaId=%d%n", subject, schemaId);
        System.out.printf("   Версия: %d%n", client.getVersion(subject, schema));
        
        return schemaId;
    }

    /**
     * Проверяет совместимость схемы с зарегистрированными.
     * 
     * @param schemaResource путь к .avsc в classpath
     * @return true, если совместима
     */
    public boolean checkCompatibility(String schemaResource) throws IOException, RestClientException {
        Schema schema = loadSchema(schemaResource);
        return client.testCompatibility(subject, schema);
    }

    /**
     * Загружает Avro-схему из файла.
     */
    private Schema loadSchema(String schemaResource) throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(schemaResource)) {
            if (is == null) {
                throw new IOException("Схема не найдена: " + schemaResource);
            }
            String schemaJson = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            return new Schema.Parser().parse(schemaJson);
        }
    }

    /**
     * Возвращает количество зарегистрированных версий.
     */
    public int getVersionCount() throws RestClientException, IOException {
        return client.getAllVersions(subject).size();
    }
}