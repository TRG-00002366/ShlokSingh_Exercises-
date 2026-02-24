"""
Robust Payment Producer
========================
Pair Programming Exercise: Build a production-ready Kafka producer.

Complete the TODO sections with your pair programming partner.
Switch Driver/Navigator roles every 25 minutes!

Prerequisites:
    pip install kafka-python
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import json
import time
import signal
import sys
from datetime import datetime
from typing import Optional

from payment_validator import validate_payment


# ============================================================
# METRICS TRACKING
# ============================================================

class ProducerMetrics:
    """Track producer performance metrics."""
    
    def __init__(self):
        self.total_sent = 0
        self.successful = 0
        self.failed = 0
        self.latencies = []
        self.start_time = None
    
    def record_success(self, latency_ms: float):
        """Record a successful send."""
        self.successful += 1
        self.latencies.append(latency_ms)
    
    def record_failure(self):
        """Record a failed send."""
        self.failed += 1
    
    def report(self):
        """Print metrics summary."""
        print("\n" + "=" * 50)
        print("PRODUCER METRICS")
        print("=" * 50)
        print(f"Total messages attempted: {self.total_sent}")
        print(f"Successful: {self.successful}")
        print(f"Failed: {self.failed}")
        
        if self.latencies:
            avg_latency = sum(self.latencies) / len(self.latencies)
            print(f"Average latency: {avg_latency:.2f} ms")
        
        if self.start_time:
            elapsed = time.time() - self.start_time
            print(f"Total time: {elapsed:.2f} seconds")


# Global metrics instance
metrics = ProducerMetrics()

# Graceful shutdown flag
running = True


def signal_handler(signum, frame):
    """Handle Ctrl+C for graceful shutdown."""
    global running
    print("\n\nGraceful shutdown initiated...")
    running = False


# ============================================================
# SESSION 1: Producer Configuration
# Driver: Partner A | Navigator: Partner B
# ============================================================

def create_producer(bootstrap_servers: str = "localhost:9092") -> Optional[KafkaProducer]:
    """
    Create a production-ready Kafka producer.
    
    TODO: Configure the producer with:
    - key_serializer: encode strings to UTF-8 bytes
    - value_serializer: JSON serialization
    - acks='all': wait for all replicas
    - enable_idempotence=True: prevent duplicates
    - retries=5: retry on transient failures
    - retry_backoff_ms=100: wait between retries
    - linger_ms=10: small batch window
    """
    print("Creating producer with high-durability configuration...")
    
    try:
        # TODO: Create and return the KafkaProducer
        # Complete the configuration based on the requirements above
        
        producer = None  # Replace with your code
        
        if producer:
            print("  [OK] Producer created successfully")
        
        return producer
        
    except NoBrokersAvailable:
        print("  [ERROR] No Kafka brokers available")
        return None
    except Exception as e:
        print(f"  [ERROR] Failed to create producer: {e}")
        return None


# ============================================================
# SESSION 2: Message Generation
# Driver: Partner B | Navigator: Partner A
# ============================================================

def generate_payment_event(payment_id: int) -> dict:
    """
    Generate a payment event.
    
    TODO: Create a payment dictionary with:
    - payment_id: formatted as "PAY-XXXX"
    - customer_id: formatted as "CUST-XXX"
    - amount: random amount between 10.00 and 1000.00
    - currency: "USD" or "EUR"
    - status: "pending"
    - timestamp: current ISO timestamp
    - merchant: pick from a list of merchants
    """
    merchants = ["Amazon", "Walmart", "Target", "BestBuy", "Costco"]
    
    # TODO: Create and return the payment event
    payment = {
        # Your code here
    }
    
    return payment


def generate_test_payments(count: int = 20) -> list:
    """
    Generate a list of test payment events.
    
    Include some edge cases:
    - Most should be valid
    - 1-2 should have invalid amounts (negative or zero)
    - 1-2 should have missing fields
    """
    payments = []
    
    for i in range(1, count + 1):
        payment = generate_payment_event(i)
        
        # TODO: Add some invalid payments for testing error handling
        # For example, payment #5 could have a negative amount
        # Payment #15 could have missing customer_id
        
        payments.append(payment)
    
    return payments


# ============================================================
# SESSION 3: Callback Implementation
# Driver: Partner A | Navigator: Partner B
# ============================================================

def on_send_success(record_metadata):
    """
    Callback for successful sends.
    
    TODO: Implement this callback to:
    1. Calculate latency (use a stored timestamp)
    2. Log the success with partition and offset
    3. Update metrics
    """
    # TODO: Implement success callback
    # Hint: record_metadata has .topic, .partition, .offset, .timestamp
    
    pass  # Replace with your code


def on_send_error(exception):
    """
    Callback for failed sends.
    
    TODO: Implement this callback to:
    1. Log the error with details
    2. Update metrics
    3. Consider alerting (print warning)
    """
    # TODO: Implement error callback
    
    pass  # Replace with your code


# ============================================================
# SESSION 4: Error Handling & Main Loop
# Driver: Partner B | Navigator: Partner A
# ============================================================

def send_payment(producer, topic: str, payment: dict) -> bool:
    """
    Send a single payment with error handling.
    
    TODO: Implement this function to:
    1. Validate the payment using validate_payment()
    2. Send with callbacks attached
    3. Handle validation errors
    4. Return True if sent, False if validation failed
    """
    global metrics
    
    # TODO: Validate payment
    # Hint: is_valid, error = validate_payment(payment)
    
    # TODO: If invalid, log error and return False
    
    # TODO: Send payment with callbacks
    # Hint: producer.send(topic, key=..., value=...).add_callback(...).add_errback(...)
    
    metrics.total_sent += 1
    
    return True  # or False if validation failed


def run_producer(producer, topic: str, payments: list):
    """
    Run the main producer loop.
    
    TODO: Implement graceful handling of:
    1. Keyboard interrupt (Ctrl+C)
    2. Connection loss during sends
    3. Proper cleanup
    """
    global running, metrics
    
    print(f"\nSending {len(payments)} payments to topic '{topic}'...")
    print("-" * 50)
    
    metrics.start_time = time.time()
    
    for payment in payments:
        if not running:
            print("Shutdown requested, stopping sends...")
            break
        
        # TODO: Call send_payment() and handle errors
        
        # Small delay for visibility
        time.sleep(0.1)
    
    # Flush remaining messages
    print("\nFlushing producer buffer...")
    producer.flush()
    
    print("-" * 50)


def main():
    """Main entry point."""
    print("=" * 60)
    print("ROBUST PAYMENT PRODUCER - Pair Programming Exercise")
    print("=" * 60)
    
    # Set up signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    topic = "payments-exercise"
    
    # Create topic first
    print("\nCreating topic (if not exists)...")
    # Note: In production, topic would be pre-created
    
    # Create producer
    producer = create_producer()
    
    if producer is None:
        print("\n[FATAL] Could not create producer. Exiting.")
        sys.exit(1)
    
    # Generate test payments
    print("\nGenerating test payments...")
    payments = generate_test_payments(20)
    print(f"  Generated {len(payments)} payment events")
    
    # Run producer
    try:
        run_producer(producer, topic, payments)
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
    finally:
        # Always report metrics
        metrics.report()
        
        # Cleanup
        producer.close()
        print("\nProducer closed.")


if __name__ == "__main__":
    main()
