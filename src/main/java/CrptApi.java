import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.Builder;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Класс для взаимодействия с CrptAPI.
 * Поддерживает создание документов с использованием семафора для ограничения количества запросов.
 */
public class CrptApi {
    private static final Logger logger = Logger.getLogger(CrptApi.class.getName());
    private static final String API_URL = "https://ismp.crpt.ru/api/v3/lk/documents/create";
    private static final int INITIAL_DELAY = 1;
    private static final int PERIOD = 1;
    private static final int POOL_SIZE = 1;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Semaphore semaphore;
    private final ScheduledExecutorService scheduler;
    private final int requestLimit;
    private final TimeUnit timeUnit;
    private final BlockingQueue<Runnable> requestQueue;

    /**
     * Инициализирует Crpt API  с заданным лимитом запросов и временной единицей для таймера.
     *
     * @param timeUnit     временная единица для таймера
     * @param requestLimit максимальное количество одновременных запросов
     */
    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        this.timeUnit = timeUnit;
        this.requestLimit = requestLimit;
        this.httpClient = HttpClient.newHttpClient();
        this.objectMapper = JsonMapper.builder().addModule(new JavaTimeModule()).build();
        this.semaphore = new Semaphore(requestLimit);
        this.scheduler = Executors.newScheduledThreadPool(POOL_SIZE);
        this.requestQueue = new LinkedBlockingQueue<>();
        startReleaseSemaphore();
    }

    /**
     * Запускает процесс освобождения семафора для запросов с фиксированной частотой.
     */
    private void startReleaseSemaphore() {
        scheduler.scheduleAtFixedRate(() -> {
            for (int i = 0; i < requestLimit; i++) {
                Runnable task = requestQueue.poll();
                if (task != null) {
                    semaphore.release();
                    task.run();
                }
            }
        }, INITIAL_DELAY, PERIOD, timeUnit);
    }

    public interface DocumentClient {
        /**
         * Создает документ на Crpt API.
         *
         * @param document  информация о документе
         * @param signature подпись для аутентификации запроса
         */
        void createDocument(Document document, String signature);
    }

    @Builder
    public static class RestDocumentClientImpl implements DocumentClient {
        private final HttpClient httpClient;
        private final ObjectMapper objectMapper;
        private final Semaphore semaphore;
        private final BlockingQueue<Runnable> requestQueue;

        @Override
        public void createDocument(Document document, String signature) {
            requestQueue.offer(() -> {
                try {
                    semaphore.acquire();
                    String requestBody = objectMapper.writeValueAsString(document);
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create(API_URL))
                            .header("Content-Type", "application/json")
                            .header("Signature", signature)
                            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                            .build();

                    httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                            .thenAccept(response -> {
                                if (response.statusCode() == 200) {
                                    logger.info("Document created successfully: " + response.body());
                                } else {
                                    logger.warning("Failed to create document: " + response.body());
                                }
                            }).exceptionally(e -> {
                                logger.log(Level.SEVERE, "Error creating document", e);
                                return null;
                            }).whenComplete((res, ex) -> semaphore.release());

                } catch (IOException | InterruptedException e) {
                    logger.log(Level.SEVERE, "Error preparing document request", e);
                } finally {
                    semaphore.release();
                }
            });
        }
    }

    @Builder
    public static class DocumentController {
        private final DocumentClient documentClient;

        public void createDocument(Document document, String signature) {
            documentClient.createDocument(document, signature);
        }
    }


    /**
     * Внутренний класс для представления документа.
     */
    @Builder
    public static class Document {
        public Description description;
        @JsonProperty("doc_id")
        public String docId;
        @JsonProperty("doc_status")
        public String docStatus;
        @JsonProperty("doc_type")
        public String docType;
        public boolean importRequest;
        @JsonProperty("ownerInn")
        public String ownerInn;
        @JsonProperty("participant_inn")
        public String participantInn;
        @JsonProperty("producerInn")
        public String producerInn;
        @JsonProperty("productioDate")
        public LocalDate productionDate;
        @JsonProperty("production_type")
        public String productionType;
        public List<Product> products;
        @JsonProperty("reg_date")
        public LocalDate regDate;
        @JsonProperty("reg_number")
        public String regNumber;

        /**
         * Внутренний класс для описания документа.
         */
        @Builder
        public static class Description {
            public final String participantInn;
        }

        /**
         * Внутренний класс для представления продукта в документе.
         */
        @Builder
        public static class Product {
            @JsonProperty("certificate_document")
            public String certificateDocument;
            @JsonProperty("certificate_document_date")
            public LocalDate certificateDocumentDate;
            @JsonProperty("certificate_document_number")
            public String certificateDocumentNumber;
            @JsonProperty("owner_inn")
            public String ownerInn;
            @JsonProperty("producer_inn")
            public String producerInn;
            @JsonProperty("production_date")
            public LocalDate productioDate;
            @JsonProperty("tnved_code")
            public String tnvedCode;
            @JsonProperty("uit_code")
            public String uitCode;
            @JsonProperty("uitu_code")
            public String uituCode;
        }

        /**
         * Перечисление типов документов.
         */
        public enum DocType {
            LP_INTRODUCE_GOODS
        }

        /**
         * Перечисление статусов документов.
         */
        public enum DocStatus {
            NEW
        }

        /**
         * Перечисление типов продуктов.
         */
        public enum ProductType {
            PRODUCT_TYPE
        }
    }

    public static void main(String[] args) {
        CrptApi crptApi = new CrptApi(TimeUnit.SECONDS, 3);

        Document.Description description = Document.Description.builder()
                .participantInn("12345")
                .build();

        Document.Product product = Document.Product.builder()
                .certificateDocument("doc")
                .certificateDocumentDate(LocalDate.of(2024, 2, 12))
                .certificateDocumentNumber("num")
                .ownerInn("1234567890")
                .producerInn("1234567890")
                .productioDate(LocalDate.of(2024, 2, 12))
                .tnvedCode("code")
                .uitCode("uit")
                .uituCode("uitu")
                .build();

        Document document = Document.builder()
                .description(description)
                .docId("1234")
                .docStatus(Document.DocStatus.NEW.name())
                .docType(Document.DocType.LP_INTRODUCE_GOODS.toString())
                .importRequest(true)
                .ownerInn("1234567890")
                .participantInn("1234567890")
                .producerInn("1234567890")
                .productionDate(LocalDate.of(2024, 2, 12))
                .productionType(Document.ProductType.PRODUCT_TYPE.name())
                .products(new ArrayList<>(List.of(product)))
                .regDate(LocalDate.of(2024, 2, 12))
                .regNumber("reg123")
                .build();

        String signature = "signature";

        DocumentClient documentClient = RestDocumentClientImpl.builder()
                .httpClient(crptApi.httpClient)
                .objectMapper(crptApi.objectMapper)
                .semaphore(crptApi.semaphore)
                .requestQueue(crptApi.requestQueue)
                .build();

        DocumentController documentController = DocumentController.builder()
                .documentClient(documentClient)
                .build();

        for (int i = 0; i < 10; i++) {
            documentController.createDocument(document, signature);
        }
    }
}