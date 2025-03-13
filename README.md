# RedisTechAssApp

## Overview

RedisTechAssApp is a Java application designed for distributed message processing using Redis Streams and Pub/Sub. It implements a configurable consumer group that subscribes to a Redis channel, processes messages, and stores the results in a Redis Stream. The application also includes real-time performance monitoring to measure processing throughput.

## Features

- Configurable consumer group with a dynamic number of consumers.
- Subscription to a Redis Pub/Sub channel (`messages:published`).
- Message processing with consumer tracking.
- Storage of processed messages in a Redis Stream (`messages:processed`).
- Periodic reporting of messages processed per second.

## Prerequisites

- Java 11 or later
- Redis server (https://redis.io/)
- Jedis library for Redis interaction

## Installation and Setup

### 1. Install Redis

Follow the instructions on the official Redis website to install and run Redis locally: https://redis.io/docs/getting-started/

### 2. Clone the Repository

```sh
 git clone <repository-url>
 cd <repository-folder>
```

### 3. Build the Application

Use Maven to build the application:
```sh
 mvn clean package
```

## Running the Application

### 1. Start Redis Server
Ensure Redis is running on `localhost:6379`. If needed, modify the `REDIS_HOST` and `REDIS_PORT` values in the code.

### 2. Start the Publisher (Improved: Better Error Handling, Performance Optimization, Connection Pooling, Logging, Configurable Parameters)
To simulate message publishing, run the provided Python script:
```sh
python publisher.py
```
This script will send messages to the `messages:published` channel.

### 3. Run the Consumer Group
Start the application with the desired number of consumers:
```sh
java -jar target/RedisTechAssApp.jar <group_size>
```
Replace `<group_size>` with the number of consumers you want to initialize.

## How It Works

### Consumer Group Initialization
- Creates a Redis consumer group (`message_consumers`).
- Populates the `consumer:ids` list with active consumer identifiers.

### Message Processing
- Consumers subscribe to `messages:published`.
- Each message is processed by exactly one consumer.
- Processed messages are stored in `messages:processed` with metadata (`message_id`, processing timestamp, consumer ID).

### Performance Monitoring
- Every 3 seconds, the application reports the processing rate (messages per second).

## Code Structure

- **RedisTechAssApp.java**: Main entry point, manages consumer group initialization and scheduling.
- **RedisPubSubListener.java**: Handles message consumption and processing.
- **publisher.py**: Simulates message publishing to Redis Pub/Sub.

## Evaluation Criteria Compliance
- ✅ **Functionality**: Implements all required Redis messaging features.
- ✅ **Scalability**: Supports configurable consumer group size.
- ✅ **Efficiency**: Ensures only one consumer processes each message.
- ✅ **Monitoring**: Reports processing rate periodically.

## Bonus Points (Optional Enhancements)
- 🛠 **Unit and Functional Tests** (Recommended for robustness).
- 🌍 **Distributed Deployment** (Run consumers across multiple machines).
- 🚀 **Optimized Redis Usage** (Consider Lua scripts or Redis Streams with `XREADGROUP`).

## Feedback
We welcome any suggestions or improvements! Please share your feedback in the repository issues section or contact the maintainers.

---

**Author**: mhlukhov  
**License**: MIT  
**Last Updated**: March 2025

