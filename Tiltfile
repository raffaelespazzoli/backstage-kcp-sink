 
# -*- mode: Python -*-
image = 'quay.io/' + os.environ['repo'] + '/backstage-kcp-sink'
custom_build(
  image,
  deps=['.'],
  command='./mvnw package -Dquarkus.container-image.build=true -Dquarkus.container-image.image=$EXPECTED_REF && buildah push $EXPECTED_REF $EXPECTED_REF',
  skips_local_docker = True,
  live_update = [
    sync('./src/', '/project/src'),
    sync('./src/pom.xml', '/project/pom.xml')  
  ],)
allow_k8s_contexts(k8s_context())
k8s_yaml('./target/kubernetes/kubernetes.yml')
k8s_resource('backstage-kcp-sink')