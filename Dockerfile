FROM python:3.10.6

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir \
    ccxt==1.93.98 \
    dataset==1.5.2 \
    psycopg2==2.9.3

ADD . /app
ENV CRYPTO_SYNC_LOG_LEVEL debug
WORKDIR /app
CMD python -m src.main
