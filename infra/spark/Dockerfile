FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu@sha256:f3d6eaa8bab8ec2e38f3c3918a5b2f8b253c95bcb19f9e87ad0e0cdacf9df2d5

ARG ICEBERG_VERSION=1.11.0

USER root

ADD --checksum=sha256:94b8e36fc329f0293d44ba9e01b784a56e9501affec1842d898144c51f6e486a \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
    /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar
ADD --checksum=sha256:38f01da7e96850cdd05e6616d758b77b43314b712a8808e3f9a824d56976162f \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

RUN chmod 0444 /opt/spark/jars/iceberg-*.jar

USER 185
