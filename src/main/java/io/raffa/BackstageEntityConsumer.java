package io.raffa;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;
import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.stream.JsonParser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import scala.collection.IterableViewLike.Mapped;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.generic.GenericRecord;

@ApplicationScoped
public class BackstageEntityConsumer {

  // @Incoming("json_final_entities")
  // @Blocking
  // public void consume(JsonNode msg) {
  // // access record metadata
  // //var metadata =
  // msg.getMetadata(IncomingKafkaRecordMetadata.class).orElseThrow();
  // // process the message payload.
  // String resourceString =
  // msg.findValue("schema.payload.after.final_entity").asText();
  // // Acknowledge the incoming message (commit the offset)
  // KubernetesClient client = new DefaultKubernetesClient();

  // try {
  // client.load(new
  // ByteArrayInputStream(resourceString.getBytes())).createOrReplace();
  // }
  // finally {
  // client.close();
  // }

  // }

  // @Incoming("avro_final_entities")
  // @Blocking
  // public void consume(GenericRecord msg) {
  // // access record metadata
  // //var metadata =
  // msg.getMetadata(IncomingKafkaRecordMetadata.class).orElseThrow();
  // // process the message payload.
  // String resourceString = (String)msg.get("schema.payload.after.final_entity");
  // // Acknowledge the incoming message (commit the offset)
  // KubernetesClient client = new DefaultKubernetesClient();

  // try {
  // client.load(new
  // ByteArrayInputStream(resourceString.getBytes())).createOrReplace();
  // }
  // finally {
  // client.close();
  // }

  // }
  private static final Logger LOG = Logger.getLogger(BackstageEntityConsumer.class);

  @Incoming("string_final_entities")
  @Blocking
  public CompletionStage<Void> consume(Message<String> msg) throws Throwable {
    // access record metadata
    var metadata = msg.getMetadata(IncomingKafkaRecordMetadata.class).orElseThrow();
    // process the message payload.
    String kPayload = msg.getPayload();
    LOG.info("received message: " + kPayload);
    KubernetesClient client = new DefaultKubernetesClient();
    try {
      Map<String, Object> resMap = new ObjectMapper().readValue(kPayload, new TypeReference<Map<String, Object>>() {
      });
      LOG.info("received map: " + resMap);
      Map<String, Object> payload=((Map<String, Object>) resMap.get("payload"));
      LOG.info("payload: " + payload);
      Map<String, Object> after=((Map<String, Object>) payload.get("after"));
      LOG.info("after: " + after);
      String finalEntityString=((String) after.get("final_entity"));
      LOG.info("final_entity: " + finalEntityString);

      client.load(new ByteArrayInputStream(finalEntityString.getBytes())).createOrReplace();
    } catch (Throwable e) {
      LOG.errorf(e, "error processing messages");
      throw e;
    } finally {
      client.close();
    }
    // Acknowledge the incoming message (commit the offset)
    return msg.ack();
  }

}