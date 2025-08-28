import configparser
import os
from kafka import KafkaProducer, KafkaConsumer
import ssl
import json

def produce_to_kafka(config):
    source_config = config['kafka_source']
    local_config = config['kafka_local']

    source_sasl_config = {
        'security_protocol': source_config['security_protocol'],
        'sasl_mechanism': source_config['sasl_mechanism'],
        'sasl_plain_username': source_config['sasl_plain_username'],
        'sasl_plain_password': source_config['sasl_plain_password']
    }
    local_sasl_config = {
        'security_protocol': local_config['security_protocol'],
        'sasl_mechanism': local_config['sasl_mechanism'],
        'sasl_plain_username': local_config['sasl_plain_username'],
        'sasl_plain_password': local_config['sasl_plain_password']
    }

    # SSL context
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    consumer = KafkaConsumer(
        source_config['topic_source'],
        bootstrap_servers=source_config['bootstrap_servers'].split(','),
        auto_offset_reset='earliest',
        ssl_context=context,
        **source_sasl_config
    )

    producer = KafkaProducer(
        bootstrap_servers=local_config['bootstrap_servers'].split(','),
        **local_sasl_config,
        value_serializer = lambda v: json.dumps(v).encode('utf-8')
    )

    topic_mapping = {
        "charge.succeeded": local_config['charge_succeeded'],
        "\\core\\event\\user_loggedin": local_config['topic_user_loggedin'],
        "\\core\\event\\user_loggedout": local_config['topic_user_loggedout'],
        "\\core\\event\\course_viewed": local_config['topic_course_viewed'],
        "\\mod_forum\\event\\discussion_viewed": local_config['topic_discussion_viewed'],
    }

    error_topic = local_config['topic_error']

    for message in consumer:
        try:
            data = json.loads(message.value.decode('utf-8'))

            if 'eventname' not in data and 'type' in data:
                data['eventname'] = data['type']
                del data['type']
            else:
                data.pop('token', None)

                int_fields = [
                    'objectid',
                    'edulevel',
                    'contextid',
                    'contextlevel',
                    'contextinstanceid',
                    'userid',
                    'courseid',
                    'relateduserid'
                ]

                for field in int_fields:
                    if field in data and data[field] not in (None, '', 'null'):
                        try:
                            data[field] = int(data[field])
                        except (ValueError, TypeError):
                            print(f"Warning: Cannot cast field '{field}' to int: {data[field]}")
                            data[field] = None

            event_name = data.get('eventname')
            target_topic = topic_mapping.get(event_name, error_topic)
            producer.send(target_topic, data)
            print(f"Produced message to {target_topic}: {data}")

        except Exception as e:
            print(f"Error producing message: {e}")
            try:
                producer.send(error_topic, {'error': str(e), 'original_message': message.value.decode('utf-8')})
                print(f"Original message sent to error topic {error_topic}")
            except Exception as e2:
                print(f"Failed to send to error topic: {e2}")

    producer.flush()

if __name__ == "__main__":
    config = configparser.ConfigParser()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, 'config.ini')

    config.read(config_path)
    produce_to_kafka(config)