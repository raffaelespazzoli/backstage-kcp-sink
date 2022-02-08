package io.raffa;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;
import javax.json.Json;
import javax.json.JsonValue;
import javax.json.stream.JsonParser;

import com.fasterxml.jackson.databind.JsonNode;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;


import org.apache.avro.generic.GenericRecord;

@ApplicationScoped
public class BackstageEntityConsumer {

    @Incoming("json_final_entities")  
    @Blocking             
    public void consume(JsonNode msg) {
      // access record metadata
      //var metadata = msg.getMetadata(IncomingKafkaRecordMetadata.class).orElseThrow();
      // process the message payload.
      String resourceString = msg.findValue("schema.payload.after.final_entity").asText();
      // Acknowledge the incoming message (commit the offset)
      KubernetesClient client = new DefaultKubernetesClient();

      try {
        client.load(new ByteArrayInputStream(resourceString.getBytes())).createOrReplace();
      }
      finally {
        client.close();
      }

  }

  @Incoming("avro_final_entities")  
  @Blocking             
  public void consume(GenericRecord msg) {
    // access record metadata
    //var metadata = msg.getMetadata(IncomingKafkaRecordMetadata.class).orElseThrow();
    // process the message payload.
    String resourceString = (String)msg.get("schema.payload.after.final_entity");
    // Acknowledge the incoming message (commit the offset)
    KubernetesClient client = new DefaultKubernetesClient();

    try {
      client.load(new ByteArrayInputStream(resourceString.getBytes())).createOrReplace();
    }
    finally {
      client.close();
    }

}  

@Incoming("string_final_entities")
@Blocking 
public CompletionStage<Void> consume(Message<String> msg) {
    // access record metadata
    var metadata = msg.getMetadata(IncomingKafkaRecordMetadata.class).orElseThrow();
    // process the message payload.
    String payload = msg.getPayload();
    JsonParser parser = Json.createParser(new StringReader(payload));
    JsonValue value = parser.getValue();
    
    String finalEntityString = value.asJsonObject().getJsonObject("schema").getJsonObject("payload").getJsonObject("after").getString("final_entity");

    KubernetesClient client = new DefaultKubernetesClient();

    try {
      client.load(new ByteArrayInputStream(finalEntityString.getBytes())).createOrReplace();
    }
    finally {
      client.close();
    }
    // Acknowledge the incoming message (commit the offset)
    return msg.ack();
}

}