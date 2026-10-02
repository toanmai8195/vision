package com.tm.vision.ingest.cdc;

import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Chạy ngay trong source: byte[] của Kafka -> BronzeRecord, kèm topic/partition/offset/timestamp (chỉ có ở đây,
 * các operator sau không thấy). Tombstone (value rỗng, Debezium gửi sau delete) không mang dữ liệu nên bị bỏ.
 */
public class BronzeRecordDeserializer implements KafkaRecordDeserializationSchema<BronzeRecord> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<BronzeRecord> out) {
        if (record.value() == null || record.value().length == 0) {
            return;
        }
        String key = record.key() == null ? null : new String(record.key(), StandardCharsets.UTF_8);
        out.collect(BronzeRecordParser.parse(record.topic(), record.partition(), record.offset(), record.timestamp(),
                key, new String(record.value(), StandardCharsets.UTF_8)));
    }

    @Override
    public TypeInformation<BronzeRecord> getProducedType() {
        return TypeInformation.of(BronzeRecord.class);
    }
}
