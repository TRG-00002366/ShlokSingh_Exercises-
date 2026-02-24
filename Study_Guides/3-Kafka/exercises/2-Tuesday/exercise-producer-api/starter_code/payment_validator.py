"""
Payment Validator
==================
Validation utilities for payment events.
This file is provided - no TODOs to complete.
"""

from typing import Tuple


def validate_payment(payment: dict) -> Tuple[bool, str]:
    """
    Validate a payment event.
    
    Returns:
        Tuple of (is_valid, error_message)
        If valid, error_message is empty string.
    """
    required_fields = ["payment_id", "customer_id", "amount", "currency", "status"]
    
    # Check required fields
    for field in required_fields:
        if field not in payment:
            return False, f"Missing required field: {field}"
        if payment[field] is None:
            return False, f"Field '{field}' is None"
    
    # Validate amount
    amount = payment.get("amount")
    if not isinstance(amount, (int, float)):
        return False, f"Amount must be numeric, got {type(amount).__name__}"
    if amount <= 0:
        return False, f"Amount must be positive, got {amount}"
    if amount > 1000000:
        return False, f"Amount exceeds maximum (1000000), got {amount}"
    
    # Validate currency
    currency = payment.get("currency")
    valid_currencies = ["USD", "EUR", "GBP", "JPY", "CAD"]
    if currency not in valid_currencies:
        return False, f"Invalid currency '{currency}'. Valid: {valid_currencies}"
    
    # Validate status
    status = payment.get("status")
    valid_statuses = ["pending", "processing", "completed", "failed"]
    if status not in valid_statuses:
        return False, f"Invalid status '{status}'. Valid: {valid_statuses}"
    
    # Validate payment_id format
    payment_id = payment.get("payment_id", "")
    if not payment_id.startswith("PAY-"):
        return False, f"Payment ID must start with 'PAY-', got '{payment_id}'"
    
    # Validate customer_id format
    customer_id = payment.get("customer_id", "")
    if not customer_id.startswith("CUST-"):
        return False, f"Customer ID must start with 'CUST-', got '{customer_id}'"
    
    return True, ""


def format_payment(payment: dict) -> str:
    """Format a payment for display."""
    return (
        f"Payment {payment.get('payment_id', 'N/A')}: "
        f"{payment.get('amount', 0):.2f} {payment.get('currency', 'N/A')} "
        f"from {payment.get('customer_id', 'N/A')} "
        f"[{payment.get('status', 'N/A')}]"
    )
