package um.si;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class OrderProducer {

    private final static String TOPIC = "kafka-views";
    private static List<Integer> tkUporabnikValues;
    private static List<Integer> tkVideovsebinaValues;

    private static void loadUniqueValues() {
        String line = "";
        String csvFile = "C:\\Users\\medve\\OneDrive - Univerza v Mariboru\\2023-2024_1.letnik MAG\\PODATKOVNE TEHNOLOGIJE IN STORITVE\\VAJE\\PRVI SKLOP\\GENERIRANE CSV DATOTEKE BAZE MYSQL\\ogledana_videovsebina_ocena.csv"; // Replace with the actual file path
        Set<Integer> uniqueTkUporabnik = new HashSet<>();
        Set<Integer> uniqueTkVideovsebina = new HashSet<>();

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            br.readLine(); // skip header line

            while ((line = br.readLine()) != null) {
                String[] values = line.split(";"); // assuming semicolon is the delimiter

                // Assuming the 'tk_uporabnik' is the third column and 'tk_videovsebina' is the fourth column
                uniqueTkUporabnik.add(Integer.parseInt(values[2].replace("\"", "")));
                uniqueTkVideovsebina.add(Integer.parseInt(values[3].replace("\"", "")));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        tkUporabnikValues = new ArrayList<>(uniqueTkUporabnik);
        tkVideovsebinaValues = new ArrayList<>(uniqueTkVideovsebina);
    }

    private static KafkaProducer createProducer() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // Configure the KafkaAvroSerializer.
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        // Schema Registry location.
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        return new KafkaProducer(props);
    }

    private static ProducerRecord<Object,Object> generateRecord(Schema schema) {
        Random rand = new Random();
        GenericRecord avroRecord = new GenericData.Record(schema);

        String orderNo = String.valueOf((int)(Math.random()*(10000 - 0 + 1) + 1));
        avroRecord.put("id",orderNo);
        avroRecord.put("ocena_filma",Integer.valueOf(rand.nextInt((10-1)) + 1));
        avroRecord.put("tk_uporabnik",tkUporabnikValues.get(rand.nextInt(tkUporabnikValues.size())));
        avroRecord.put("tk_videovsebina",tkVideovsebinaValues.get(rand.nextInt(tkVideovsebinaValues.size())));
        avroRecord.put("dodano",System.currentTimeMillis());

        ProducerRecord<Object, Object> producerRecord = new ProducerRecord<>(TOPIC, orderNo, avroRecord);
        return producerRecord;
    }


    public static void main(String[] args) throws Exception {
        loadUniqueValues();

        Schema schema = SchemaBuilder.record("Views")
                .fields()
                .requiredString("id")
                .requiredInt("ocena_filma")
                .requiredInt("tk_uporabnik")
                .requiredInt("tk_videovsebina")
                .requiredLong("dodano")
                .endRecord();

       KafkaProducer producer = createProducer();


        while(true){
            ProducerRecord record = generateRecord(schema);
            producer.send(record);

            System.out.println("[RECORD] Sent new view object.");
            Thread.sleep(10000);
        }
    }

}
