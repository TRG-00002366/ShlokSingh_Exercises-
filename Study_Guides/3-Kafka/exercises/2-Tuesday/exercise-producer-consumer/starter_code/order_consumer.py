"""
Order Consumer
===============
Complete the TODO sections to consume order messages from Kafka.

Prerequisites:
    pip install kafka-python
"""

from kafka import KafkaConsumer
import json
import signal
import sys


# Flag for graceful shutdown
running = True


def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully."""
    global running
    print("\n\nShutting down consumer...")
    running = False


def create_consumer(topic: str, bootstrap_servers: str = "localhost:9092"):
    """
    Create a KafkaConsumer with JSON deserialization.
    
    TODO: Create and return a KafkaConsumer with:
    - group_id for consumer group
    - auto_offset_reset to read from beginning
    - value_deserializer for JSON
    """
    # TODO: Create and return the consumer
    # Hint:
    #   KafkaConsumer(
    #       topic,
    #       bootstrap_servers=...,
    #       group_id='order-processors',
    #       auto_offset_reset='earliest',
    #       enable_auto_commit=True,
    #       value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    #   )
    
    pass  # Replace with your code


def process_order(order: dict, partition: int, offset: int):
    """
    Process a received order.
    
    TODO: Print the order details in a formatted way.
    """
    print("\nReceived Order:")
    print("-" * 30)
    
    # TODO: Print order details
    # - Order ID
    # - Customer ID  
    # - Product
    # - Quantity
    # - Price (formatted as currency)
    # - Partition and Offset
    
    # Your code here
    pass


def consume_orders(consumer):
    """
    Consume and process orders from the topic.
    
    TODO: Complete this function to:
    1. Iterate over messages from the consumer
    2. Extract order data from each message
    3. Call process_order() for each message
    4. Handle graceful shutdown
    """
    global running
    
    print("Waiting for orders... (Press Ctrl+C to stop)")
    print("=" * 50)
    
    order_count = 0
    
    # TODO: Iterate over messages from consumer
    # Hint: for message in consumer:
    #           if not running: break
    #           order = message.value
    #           process_order(order, message.partition, message.offset)
    
    # Your code here
    pass
    
    print("=" * 50)
    print(f"\nConsumed {order_count} orders total")


def main():
    """Main function to run the order consumer."""
    print("=" * 50)
    print("ORDER CONSUMER")
    print("=" * 50)
    
    # Set up signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    topic = "orders-exercise"
    
    print(f"\nConnecting to topic '{topic}'...")
    
    # Create consumer
    consumer = create_consumer(topic)
    
    if consumer is None:
        print("\n[ERROR] Consumer not created. Complete the TODO!")
        return
    
    # Consume orders
    consume_orders(consumer)
    
    # Cleanup
    consumer.close()
    
    print("\n" + "=" * 50)
    print("CONSUMER COMPLETE")
    print("=" * 50)


if __name__ == "__main__":
    main()
