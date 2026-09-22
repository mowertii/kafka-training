package ru.otus.kafka.hw6.schema;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Проверка совместимости Avro-схем.
 * 
 * BACKWARD совместимость:
 * - Новая схема может читать данные, записанные старой схемой.
 * - Можно удалять поля (если у них есть default).
 * - Можно добавлять поля (если у них есть default).
 * 
 * НЕсовместимые изменения:
 * - Изменение типа поля (int → string)
 * - Переименование поля без alias
 * - Удаление поля без default
 */
public class SchemaCompatibilityCheck {

    private final SchemaRegistryClient client;
    private final String subject;

    public SchemaCompatibilityCheck(String schemaRegistryUrl, String subject) {
        this.client = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
        this.subject = subject;
    }

    /**
     * Проверяет совместимость схемы.
     * 
     * @param schemaResource путь к .avsc в classpath
     * @return true, если совместима
     */
    public boolean check(String schemaResource) throws IOException, RestClientException {
        Schema schema = loadSchema(schemaResource);
        return client.testCompatibility(subject, schema);
    }

    /**
     * Пытается зарегистрировать схему (если несовместима — исключение).
     * 
     * @return schema ID, если успешно
     */
    public int tryRegister(String schemaResource) throws IOException, RestClientException {
        Schema schema = loadSchema(schemaResource);
        return client.register(subject, schema);
    }

    /**
     * Получает информацию о схеме.
     */
    public String getSchemaInfo(String schemaResource) throws IOException, RestClientException {
        Schema schema = loadSchema(schemaResource);
        boolean compatible = client.testCompatibility(subject, schema);
        return String.format("Subject: %s, Compatible: %s", subject, compatible);
    }

    private Schema loadSchema(String schemaResource) throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(schemaResource)) {
            if (is == null) {
                throw new IOException("Схема не найдена: " + schemaResource);
            }
            String schemaJson = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            return new Schema.Parser().parse(schemaJson);
        }
    }
}