from talos.config import Settings

from subreddit_rescanner import SubredditRescanner

if __name__ == "__main__":
    subreddit_rescanner = SubredditRescanner(
        retry_attempts=3,
        time_between_attempts=10,
        producing_queue=Settings.SUBREDDIT_RESCAN_QUEUE
    )
    
    subreddit_rescanner.run()
