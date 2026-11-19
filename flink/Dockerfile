# Image Flink + Python (PyFlink) supaya job Python bisa di-submit dengan `flink run -py`.
FROM flink:1.20-java17

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip python3-dev \
 && ln -sf /usr/bin/python3 /usr/bin/python \
 && rm -rf /var/lib/apt/lists/*

# Versi apache-flink harus sama dengan versi cluster (1.20.x)
RUN pip3 install --no-cache-dir apache-flink==1.20.1
USER flink
