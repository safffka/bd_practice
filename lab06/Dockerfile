FROM spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

RUN mkdir -p /opt/spark/work-dir /opt/spark/ivy \
    && chown -R spark:spark /opt/spark

COPY script.py /opt/spark/work-dir/script.py
COPY chinook.db /opt/spark/work-dir/chinook.db

USER spark

ENTRYPOINT ["/opt/spark/bin/spark-submit","--packages","org.xerial:sqlite-jdbc:3.45.1.0,org.postgresql:postgresql:42.7.3","--conf","spark.jars.ivy=/opt/spark/ivy","/opt/spark/work-dir/script.py"]
