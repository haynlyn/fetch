from confluent_kafka import Consumer, Producer
import json
import logging
import os
import pandas as pd
from datetime import datetime
from collections import defaultdict

BATCH_SIZE_C = int(os.environ.get('BATCH_SIZE_C', 100))
BATCH_DURATION_S = int(os.environ.get('BATCH_DURATION_S', 10))
INPUT_TOPIC = os.environ.get('INPUT_TOPIC', "user-login")
OUTPUT_TOPIC = os.environ.get('OUTPUT_TOPIC', "login-metrics")
BOOTSTRAP_SERVERS = os.environ.get('BOOTSTRAP_SERVERS', 'kafka:9092')

# Define logger for all steps in this pipeline.
# This could also be streamed, for real-time visibility of pipeline health.
logger = logging.getLogger(__name__)
logging.basicConfig(filename=f'{OUTPUT_TOPIC}.log', encoding='utf-8', level=logging.DEBUG,
                    datefmt='%Y-%m-%d %H:%M:%S', format='%(asctime)s %(message)s')
logger.addHandler(logging.StreamHandler())

def group_agg(df, func='size', keys=None):
    """
    Helper function: performs simple or arbitrary
    aggregation for batch analysis.
    """
    if keys == None:
        return None

    res = df.fillna('na').groupby(by=keys).agg(func)
    return res

def analyze_batch(df):
    """
    Define a set of keys-groups on which to perform certain analyses.
    For now, mainly focusing on 'size' w.r.t. aggregates.

    I.e., given a desired slice of the data, only record instances of
    duplicates.
    """
    key_combos = (
        ['ip'], # Can aid in detecting suspicious activity if an IP address count is too high.
        ['user_id', 'ip'], # May help for marketing efforts. Can be used for fraud detection.
        ['user_id', 'device_id'], # Can be used for fraud detection.
        ['app_version'], # Helps to assess distribution of software.
        ['app_version', 'locale'], # Can help in determining where to introduce rollout/upgrade.
        ['app_version', 'device_type'], # Same as above.
        ['locale', 'device_type'], # Same as above.
    )

    aggs = []
    for kcombo in key_combos:
        # Analyze batch across different slices.
        dupdf = group_agg(df, 'size', kcombo)
        # Drop unique records.
        dupdf = dupdf[dupdf > 1]
        dupdf.sort_values(ascending=False, inplace=True)
        dupdf = dupdf.reset_index() # Cannot be in-place because Series -> DataFrame no bueno.
        dupdf.rename(columns={0: 'size'}, inplace=True)
        
        # If there's actual data, append it to the list.
        if not dupdf.empty:
            df_json = dupdf.to_json(orient='records', index='True')
            data = {'key': kcombo, 'value': df_json}
            aggs.append(data)

    return aggs

def make_consumer(topics=[INPUT_TOPIC]):
    config = {
            # 'bootstrap.servers': 'localhost:29092',
            'bootstrap.servers': BOOTSTRAP_SERVERS,
            'group.id': 'fetch',
            'session.timeout.ms': 6000,
            'auto.offset.reset': 'latest', # 'latest' for fresh, 'earliest' for robust. I think there should be 1 `earliest` to play catch up in case.
            'enable.auto.commit': False, # Processing here may take too long. Manually commit.
            'enable.auto.offset.store': True, # In the event of failure
            'security.protocol': 'plaintext', # Needed to communicate with Producer from Fetch.
            }

    c = Consumer(config)
    c.subscribe(topics)
    return c

def make_producer():
    profig = {
              # 'bootstrap.servers': 'localhost:29092',
              'bootstrap.servers': BOOTSTRAP_SERVERS,
              'compression.codec': 'gzip',
              'logger': logger,
              # 'security.protocol': 'ssl', # Not needed for assessment but should be used in production
              # 'ssl.key.location': path/to/private/key.pem
              # 'ssl.certificate.location': path/to/public/key.pem
                ## For these, we need to ensure that OpenSSL is installed and set up
                ## TODO: Look up anything else that may be needed for implementing SSL protocol in Kafka
            }

    p = Producer(profig)
    return p

def main():
    logger.info("Starting stream.")
    logger.info("Make consumer")
    c = make_consumer()
    logger.info("Consumer made")
    logger.info("Make producer")
    p = make_producer()
    logger.info("Producer made")

    try:
        logger.info(f"Beginning message consumption loop.")
        # Set default values for determining the window of the batch.
        ts_min, ts_max = None, None
        while True:
            # Ensures that messages will be had when either of these limits are met.
            msgs = c.consume(num_messages=BATCH_SIZE_C, timeout=BATCH_DURATION_S)
            
            # Iterate through messages. Log status, but process valid messages.
            for msg in msgs:
                msg_index = msgs.index(msg)
                if msg.error():
                    logger.info(f'KafkaError: {msg.error()}')
                    msgs.pop(msg_index)
                else:
                    logger.info(f"Decoding message: {msg.key()}")
                    data = json.loads(msg.value().decode('utf-8'))
                    msgs[msg_index] = data
                    ts = data['timestamp']

                    ts_min = ts if not ts_min else min(ts, ts_min)
                    ts_max = ts if not ts_max else max(ts, ts_max)
                    
            # Perform analysis.
            logger.info("Performing mini-batch analysis.")
            try:
                # I use Pandas and pd.concat() to coerce all records together. New schemas are merged.
                df = pd.concat([pd.DataFrame(msg, index=[0]) for msg in msgs], ignore_index=True)
                aggs = analyze_batch(df)
                if aggs:
                    res = {'metrics': aggs}
                    res['ts_min'] = ts_min
                    res['ts_max'] = ts_max
                    value = json.dumps(res)
                    logger.info(f'Producing metrics to {OUTPUT_TOPIC} in round-robin fashion.')
                    logger.info(f"Metrics values: {value}")
                    p.produce(OUTPUT_TOPIC, value=value, timestamp=int(datetime.now().timestamp()))
                else:
                    logger.info("Empty insights. Dumping batch.")
            except Exception as e:
                logger.error(f"Error analyzing and writing data: {e}")
            finally:
                logger.info("Preparing new batch.")
                ts_min, ts_max = None, None
                c.commit()

    except KeyboardInterrupt:
        logger.info(f"Ending stream.")
    except Exception as e:
        logger.error(e)
    finally:
        p.flush()
        c.close()

if __name__ == '__main__':
    main()
