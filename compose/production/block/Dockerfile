
FROM python:3.7-alpine

ENV PYTHONUNBUFFERED 1

RUN apk add --no-cache --virtual build-deps python3-dev musl-dev gcc linux-headers && \
  apk add --no-cache postgresql-dev zlib-dev

RUN addgroup -S block \
  && adduser -S -G block block


# Requirements are installed here to ensure they will be cached.
COPY ./requirements /requirements
RUN pip install --no-cache-dir -r /requirements/production.txt \
  && rm -rf /requirements

RUN apk del build-deps

COPY ./compose/production/block/entrypoint /entrypoint
RUN sed -i 's/\r$//g' /entrypoint
RUN chmod +x /entrypoint
RUN chown block /entrypoint

COPY ./compose/production/block/start /start
RUN sed -i 's/\r$//g' /start
RUN chmod +x /start
RUN chown block /start
COPY . /app

RUN chown -R block /app
RUN mkdir /storage && chown -R block /storage

USER block

WORKDIR /app

ENTRYPOINT ["/entrypoint"]
