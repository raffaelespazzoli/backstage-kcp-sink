package io.raffa;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import io.fabric8.kubernetes.api.model.APIResourceList;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext;
import io.fabric8.kubernetes.client.utils.ApiVersionUtil;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

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
  private static final Logger LOG = Logger.getLogger(BackstageEntityConsumer.class);

  @ConfigProperty(name = "token-file-name") 
  String tokenFileName;

  @ConfigProperty(name = "ca-crt-file-name") 
  String cacrtFileName;

  @ConfigProperty(name = "kcp-url") 
  String kcpUrl;

  @Inject
  KubernetesClient managedClient;

  @Incoming("string_final_entities")
  @Blocking
  public CompletionStage<Void> consume(Message<String> msg) throws Throwable {
    // access record metadata
    var metadata = msg.getMetadata(IncomingKafkaRecordMetadata.class).orElseThrow();
    // process the message payload.
    String kPayload = msg.getPayload();
    LOG.info("received message: " + kPayload);

    String token = Files.readString(Path.of(tokenFileName));
    Config kubeClientConfig=managedClient.getConfiguration();
    kubeClientConfig.setOauthToken(token);
    kubeClientConfig.setCaCertFile(cacrtFileName);
    kubeClientConfig.setMasterUrl(kcpUrl);
    kubeClientConfig.setDisableHostnameVerification(true);
    kubeClientConfig.setNamespace("default");
    KubernetesClient client = new DefaultKubernetesClient(kubeClientConfig);
    
    try {
      Map<String, Object> resMap = new ObjectMapper().readValue(kPayload, new TypeReference<Map<String, Object>>() {
      });
      //LOG.info("received map: " + resMap);
      Map<String, Object> payload = ((Map<String, Object>) resMap.get("payload"));
      //LOG.info("payload: " + payload);
      if (payload == null) {
        LOG.info("empty payload, returning");
        return msg.ack();
      }
      String op = ((String) payload.get("op"));
      //LOG.info("op: " + op);
      if (!Arrays.asList(new String[] { "r", "u", "d", "i" }).contains(op)) {
        LOG.info("unrecognized op, returing: " + op);
        return msg.ack();
      }
      Map<String, Object> after = ((Map<String, Object>) payload.get("after"));
      //LOG.info("after: " + after);
      if (after == null) {
        LOG.info("empty after, returning");
        return msg.ack();
      }
      String finalEntityString = ((String) after.get("final_entity"));
      LOG.info("final_entity: " + finalEntityString);
      if (finalEntityString == null || finalEntityString=="") {
        LOG.info("empty finalEntityString, returning");
        return msg.ack();
      }
      
      GenericKubernetesResource genericResource = (GenericKubernetesResource) Serialization.unmarshal(new ByteArrayInputStream(finalEntityString.getBytes()));
      if (genericResource.getKind().equals("Template")) {
        // templates are discarded
        return msg.ack();
      }
      genericResource.setMetadata(new ObjectMetaBuilder().withName(genericResource.getMetadata().getName()).withNamespace(genericResource.getMetadata().getNamespace()).withAnnotations(genericResource.getMetadata().getAnnotations()).withLabels(genericResource.getMetadata().getLabels()).build());
      ResourceDefinitionContext context = getGenericResourceContext(client, genericResource)
              .orElseThrow(() -> new IllegalStateException("Could not retrieve API resource information for:"
                      + genericResource.getApiVersion() + " " + genericResource.getKind() + ". Is the CRD for the resource available?"));

      client.genericKubernetesResources(context).withName(genericResource.getMetadata().getName())
              .createOrReplace(genericResource);

      if (op == "d") {
        client.genericKubernetesResources(context).withName(genericResource.getMetadata().getName())
        .delete();
        //client.load(new ByteArrayInputStream(finalEntityString.getBytes())).delete();
      } else {
        client.genericKubernetesResources(context).withName(genericResource.getMetadata().getName())
        .createOrReplace(genericResource);
        //client.load(new ByteArrayInputStream(finalEntityString.getBytes())).createOrReplace();
      }

    } catch (Throwable e) {
      LOG.errorf(e, "error processing messages");
      throw e;
    } finally {
      client.close();
    }
    // Acknowledge the incoming message (commit the offset)
    return msg.ack();
  }

      /**
     * Obtain everything the APIResourceList from the server and extract all the info we need in order to know how to create /
     * delete the specified generic resource.
     * 
     * @param client the client instance to use to query the server.
     * @param resource the generic resource.
     * @return an optional {@link ResourceDefinitionContext} with the resource info or empty if resource could not be matched.
     */
    private static Optional<ResourceDefinitionContext> getGenericResourceContext(KubernetesClient client,
            GenericKubernetesResource resource) {
        APIResourceList apiResourceList = client.getApiResources(resource.getApiVersion());
        if (apiResourceList == null || apiResourceList.getResources() == null || apiResourceList.getResources().isEmpty()) {
            return Optional.empty();
        }
        return client.getApiResources(resource.getApiVersion()).getResources().stream()
                .filter(r -> r.getKind().equals(resource.getKind()))
                .map(r -> new ResourceDefinitionContext.Builder()
                        .withGroup(ApiVersionUtil.trimGroup(resource.getApiVersion()))
                        .withVersion(ApiVersionUtil.trimVersion(resource.getApiVersion()))
                        .withKind(r.getKind())
                        .withNamespaced(r.getNamespaced())
                        .withPlural(r.getName())
                        .build())
                .findFirst();
    }

}