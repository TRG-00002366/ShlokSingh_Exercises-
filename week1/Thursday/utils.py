"""Utility functions for sales processing."""

def calculate_revenue(price, quantity):
    return price * quantity

def format_currency(amount):
    return f"${amount:,.2f}"