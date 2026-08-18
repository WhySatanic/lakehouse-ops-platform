# syntax=docker/dockerfile:1.7

FROM apache/hive:4.0.1

USER root
ADD --checksum=sha256:6e0e4cc2d8cae902084f8a2b18728b073a6fd9d1f87c9d8bff8f298c18185b93 \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar \
    /opt/hive/lib/postgres.jar
RUN chmod 0444 /opt/hive/lib/postgres.jar
USER hive
