FROM spark:3.5.3-scala2.12-java17-python3-ubuntu


USER root


COPY script.py /opt/spark/work-dir/script.py
COPY ml-100k/u.data /opt/spark/work-dir/u.data


RUN chmod 644 /opt/spark/work-dir/u.data && \
    chmod 644 /opt/spark/work-dir/script.py


USER 185

ENTRYPOINT ["/opt/spark/bin/spark-submit", "/opt/spark/work-dir/script.py"]
