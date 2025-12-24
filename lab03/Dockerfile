FROM spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

COPY script.py /opt/spark/work-dir/script.py
COPY world-cities.csv /opt/spark/work-dir/world-cities.csv

RUN chmod 644 /opt/spark/work-dir/script.py && \
    chmod 644 /opt/spark/work-dir/world-cities.csv

USER 185

ENTRYPOINT ["/opt/spark/bin/spark-submit", "/opt/spark/work-dir/script.py"]