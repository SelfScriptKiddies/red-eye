FROM postgres:15

ENV POSTGRES_USER=${POSTGRES_USER}
ENV POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
ENV POSTGRES_DB=${POSTGRES_DB}

RUN mkdir -p /docker-entrypoint-initdb.d
COPY init/ /docker-entrypoint-initdb.d/
RUN chmod -R 755 /docker-entrypoint-initdb.d/

EXPOSE 5432
