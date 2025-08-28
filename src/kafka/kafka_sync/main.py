import configparser
import os
import sys
import threading
from src import producer_kafka

def main():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, 'src', 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_path)

    sys.path.append(os.path.join(current_dir, 'src'))

    producer_thread = threading.Thread(target=producer_kafka.produce_to_kafka, args=(config,))

    producer_thread.start()

    producer_thread.join()

if __name__ == "__main__":
    main()