from talos.config import Settings

from rescanner import Rescanner

if __name__ == "__main__":
    rescanner_post_scraper = Rescanner(
        retry_attempts=3,
        time_between_attempts=5,
        producing_queue=Settings.RESCAN_QUEUE
    )
    
    rescanner_post_scraper.run()
