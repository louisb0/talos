from rescan_producer import RescanProducer

if __name__ == "__main__":
    rescan_producer = RescanProducer(retry_attempts=3, time_between_attempts=5)
    rescan_producer.run()