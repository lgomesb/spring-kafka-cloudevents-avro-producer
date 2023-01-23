package pt.com.poc.cloudevents.avro.kafkacloudeventsavroproducer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Properties;
import java.util.UUID;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.github.kattlo.cloudevents.AvroCloudEventData;
import io.github.kattlo.cloudevents.KafkaAvroCloudEventSerializer;
import pt.com.poc.cloudevents.avro.kafkacloudeventsavroproducer.model.TaxPayer;

@SpringBootApplication
public class KafkaCloudeventsAvroProducerApplication implements CommandLineRunner {

	private static final String TOPIC = "taxpayer-avro-topic";

	public static void main(String[] args) {
		SpringApplication.run(KafkaCloudeventsAvroProducerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		
		sendEventCloudEventsAvro();
	}

	private void sendEventCloudEventsAvro() throws IOException {
		
		Properties prop = getPropertiesCloudEventsAvro();

		TaxPayer taxPayer = TaxPayer.newBuilder()
		.setId(UUID.randomUUID().toString())
		.setName("Test")
		.setDocument("Valor do Documento")
		.setSituation(true)
		.build();

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DatumWriter<TaxPayer> writer = new GenericDatumWriter<>(taxPayer.getSchema());
		Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		writer.write(taxPayer, encoder);
		encoder.flush();
		byte[] data = out.toByteArray();

		CloudEvent event = CloudEventBuilder.v1()
		.withId(UUID.randomUUID().toString())
    	.withSource(URI.create("/example"))
    	.withType("type.example")
    	.withTime(OffsetDateTime.now())
    	.withData(AvroCloudEventData.MIME_TYPE, data)
    	.build();

		AvroCloudEventData avroCloudEventData = new AvroCloudEventData<IndexedRecord>(taxPayer);

		ProducerRecord<String, CloudEvent> record = new ProducerRecord<>(TOPIC, event);
		KafkaProducer<String, CloudEvent> producer = new KafkaProducer<>(prop);
		producer.send(record);
	
	}

	private Properties getPropertiesCloudEventsAvro() {
		Properties properties = new Properties();
		
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroCloudEventSerializer.class);
        properties.put("schema.registry.url", "http://127.0.0.1:8081" );
		properties.put("cloudevents.serializer.encoding", "BINARY");
		properties.put("auto.register.schemas", "true");
		
		return properties;
	}

}
