from talos.config import Settings

from post_rescanner import PostRescanner

if __name__ == "__main__":
    post_rescanner = PostRescanner(
        retry_attempts=3,
        time_between_attempts=10,
        producing_queue=Settings.POST_RESCAN_QUEUE
    )

    post_rescanner.run()
