# syntax=docker/dockerfile:1.7

FROM apache/hive:4.0.0

USER root
ADD --checksum=sha256:6e0e4cc2d8cae902084f8a2b18728b073a6fd9d1f87c9d8bff8f298c18185b93 \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar \
    /opt/hive/lib/postgres.jar
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar \
    /opt/hive/lib/hadoop-aws-3.3.6.jar
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar \
    /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar
RUN printf '%s  %s\n' \
      d5e162564701848b0921b80aedef9e64435333cc /opt/hive/lib/hadoop-aws-3.3.6.jar \
      929e3d336686f926253907a0efaa417ad378d820 /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar \
      | sha1sum --check --strict - \
    && chmod 0444 \
      /opt/hive/lib/postgres.jar \
      /opt/hive/lib/hadoop-aws-3.3.6.jar \
      /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar
USER hive
