# talos

talos is a distributed Reddit scraper built using Python, Docker, RabbitMQ and PostgresQL.

## Services

### 1. Subreddit Rescanner
- Reads from a queue of rescan tasks.
- Periodically rescans the last 500 posts from a subreddit - frequency determined by subreddit post rate.
- New posts discovered during the rescan are scheduled in a database.

### 2. Rescan Producer
- Monitors two tables: one for scheduled subreddit rescans and another for individual post rescans.
- Queues tasks for both the Subreddit Rescanner and the Post Rescanner based on these schedules.

### 3. Post Rescanner
- A recursive service that receives tasks to fetch top-level comments and post metadata.
- Queues additional tasks to fetch nested comments hidden under "show more" and similar structures.

### Scalability

All services are independent of one another.

In a scalable deployment, generally only one subreddit rescanner and rescan producer are required. The post rescanner does most of the work, and takes input from one queue, so this can be autoscaled based off the number of items in this queue.

## Directory Overview

- **pipeline** - Core processing modules, including data extraction and utility functions - for processing scraped data into a clean CSV format.
- **source** - Docker configurations for the services and the main utility library, central to all services.
- **tests** - Unit tests for the utility library shared amongst Docker services.
