# Fetch DE Take Home

## Overview
1) Setup
2) Instructions
3) Design Decisions
4) Processing Decisions
5) Pipeline Considerations
6) Scalability and Durability
7) Addressing Additional Questions

## 1) Setup
I developed this on my laptop, using EndeavourOS Linux, with Linux kernel `6.12.1-arch1-1`, Docker version `27.3.1` and Docker Compose version `2.30.3`.

This was also tested on my server, running Ubuntu `24.04.1 LTS` with Docker version `27.3.1 ce` and Docker Compose version `2.29.7`.

This was written using Python `3.12.7` and pip `24.3.1`.

## 2) Instructions
0) Ensure that you have the correct versions of {Docker and Docker Compose} or {Python and pip} installed.
1) Download/clone repository.
2) If executing via Docker, do the following:
    1) `docker compose -f docker-compose.yaml -f docker-compose-daniel.yaml up -d --build`
        > This executes the pipeline.
    2) `docker compose -f docker-compose.yaml -f docker-compose-daniel.yaml logs -f data-transformer`
        > This allows you to observe writes to the `login-metrics` Kafka topic.
3) If executing via Python, do the following:
    1) `python3 -m venv .venv`
    2) `source .venv/bin/activate` if you're using a Bash shell
    2) `pip install -r requirements.txt`
    3) `python3 cap.py` # Consumer And Producer
    4) Watch the metrics come in!
        * Then, use your favorite language to create a Kafka consumer using `localhost:29092` as the bootstrap server, subscribe to the `login-metrics` topic, and watch the metrics roll in!
        * Alternatively, in a Bash shell, run `tail -f login-metrics.log`

## 3) Design Decisions
I opted to use the Confluent Kafka Python package, as it is more regularly maintained than the `kafka-python` package. For longevitity in regards to designing a service, this is preferred since both need to adhere to the Kafka standard, but this has better package health.

Additionally, I opted for introducing a logger via the `logging` framework in the standard library
throughout this development. Realistically, this is something that would either also be streamed to another topic, or directly into a database.

I always advocate for keeping raw data until I'm certain there's no need of it. In practice, as long as the `user-logins` exists, one could consume from that for the raw feed. Thus, I assumed there would be such a consumer elsewhere and I focused on merely processing the data.

## 4) Processing Decisions
I chose to focus on processing the data to look for instances of duplicate data across a combination of slices. To do this, I wanted to grab batches of data that fell within the limits outlined by `BATCH_SIZE_C`, for the number of messages in a batch, and `BATCH_DURATION`, for the window of time between the earliest and latest message of the batch.

As each message could be errorneous, the list of messages is iterated through, and any error is logged via the logger and then removed from the batch. The remaining batch is then coerced into a Pandas DataFrame through a series of steps, all of which are to ensure that the schema from each message's values are merged, and that any missing data are filled with a filler value.

Then, this DataFrame is analyzed under a set of slices, such as `('user_id', 'device_id')`, where the goal is to see how many duplicates there are. Currently, such slices include some of: `('user_id', 'ip'), ('app_version', 'locale')` and `('app_version', 'device_type')`. Any unique values, specified by a `size` of `1`, are dropped, the DataFrame coerced to JSON, then encoded and sent through a Producer to a new topic, `login-metrics`.

The sent packet has the DataFrames-as-JSONs for each slice under the `'metrics'` key, as well as the timeframe window that outlines the batch, via `'ts_min'` and `'ts_max'`. Upon converting this to ISO format for datetimes/timestamps, we 

## 5) Pipeline Considerations
I opted to use `enable.auto.commit = false` because there was the risk of too long a delay in processing the messages, through conducting many aggregates on the data. This would result in possible duplication of efforts if there were ever an issue with the consumer. So, I had the consumer commit after successful processing.

My philosophy was also to have `auto.offset.reset = latest`, as this pipeline doesn't seem business critical per se, and we could afford to miss some data as long as there are fresh data. As in, I would rather the consumer continue processing new data than having to start from an earlier offset.

In a production setting, I would probably opt to have a few consumers where `auto.offset.reset = latest` but have one where `auto.offset.reset = earliest` so that any gaps can be filled, if necessary.

Additionally, I set my producer to compress the data via `gzip`, as it should probably always be compressed. I shall address the other preferences in section `7`.

## 6) Scalability and Durability
This is single-threaded, so it's not that scalable. However, there appears to be only 1 partition, so another consumer would just be idle if it's in the same group. Making multiple groups would split the data and that felt contrary to the point of the analysis. This would have to be rewritten to ensure that new consumer groups can easily be added.

The pipeline is durable in that it catches general Errors and KafkaErrors and logs them accordingly. It allows for graceful termination of the pipeline in flushing the producer and closing the consumer when the loop/program/stream is ending.

## 7) Addressing Additional Questions
1) How would you deploy this application in production?
    * ECS Cluster using the images provided here. The underlying application is written in Python, which I know, and source from the Confluent images, which are trusted maintainers of the Kafka standard on Python.
2) What other components would you want to add to make this production ready?
    * On top of compressing the data, which I opted to do for the assessment, I would encrypt them. There's the option to encrypt via a `security.protocol`. Doing so would require configuring the broker to allow for the extra overhead that the messages will require. Granted, there wasn't any sample data that suggested sensitive info, as user data was obfuscated.
    * I would implement multi-threading and allow for each thread to run their own consumer for each partition of the `user-login` topic, assuming there are many partitions.
    * A database or processor to which to feed this data, as ksqlDB, Faust (for Python) or Kafka Streams (for JVM)
3) How can this application scale with a growing dataset?
    * By using a proper stream processing framework, as mentioned above, and doing analyses therein.
    * By increasing the number of topic partitions and consumers within each group accessing the topic, through multithreading.
