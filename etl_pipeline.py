#!/usr/bin/env python3
"""
Basic ETL Pipeline for Bank Transactions
Extracts data from CSV, Transforms it, and Loads to output
"""

import csv
from datetime import datetime
from collections import defaultdict
import json


def extract(csv_file):
    """Extract data from CSV file"""
    transactions = []
    try:
        with open(csv_file, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                transactions.append({
                    'sender': row['sender'],
                    'recipient': row['recipient'],
                    'amount': float(row['amount']),
                    'timestamp': row['timestamp']
                })
        print(f"✓ Extracted {len(transactions)} transactions from {csv_file}")
        return transactions
    except FileNotFoundError:
        print(f"✗ Error: File {csv_file} not found")
        return []
    except Exception as e:
        print(f"✗ Error extracting data: {e}")
        return []


def transform(transactions):
    """Transform the transaction data"""
    if not transactions:
        return []
    
    transformed = []
    total_amount = 0
    sender_counts = defaultdict(int)
    recipient_counts = defaultdict(int)
    
    for transaction in transactions:
        # Add transformed fields
        transaction['amount_usd'] = f"${transaction['amount']:.2f}"
        
        # Parse timestamp
        try:
            dt = datetime.strptime(transaction['timestamp'], '%Y-%m-%d %H:%M:%S')
            transaction['date'] = dt.strftime('%Y-%m-%d')
            transaction['time'] = dt.strftime('%H:%M:%S')
            transaction['day_of_week'] = dt.strftime('%A')
        except ValueError:
            transaction['date'] = 'Unknown'
            transaction['time'] = 'Unknown'
            transaction['day_of_week'] = 'Unknown'
        
        # Calculate statistics
        total_amount += transaction['amount']
        sender_counts[transaction['sender']] += 1
        recipient_counts[transaction['recipient']] += 1
        
        transformed.append(transaction)
    
    # Add summary statistics
    summary = {
        'total_transactions': len(transformed),
        'total_amount': total_amount,
        'average_amount': total_amount / len(transformed) if transformed else 0,
        'top_senders': dict(sorted(sender_counts.items(), key=lambda x: x[1], reverse=True)[:5]),
        'top_recipients': dict(sorted(recipient_counts.items(), key=lambda x: x[1], reverse=True)[:5])
    }
    
    print(f"✓ Transformed {len(transformed)} transactions")
    print(f"  Total amount: ${total_amount:,.2f}")
    print(f"  Average amount: ${summary['average_amount']:.2f}")
    
    return transformed, summary


def load(transformed_data, summary, output_file='transformed_transactions.json'):
    """Load transformed data to JSON file"""
    try:
        output = {
            'summary': summary,
            'transactions': transformed_data
        }
        
        with open(output_file, 'w') as f:
            json.dump(output, f, indent=2)
        
        print(f"✓ Loaded data to {output_file}")
        return True
    except Exception as e:
        print(f"✗ Error loading data: {e}")
        return False


def main():
    """Main ETL pipeline execution"""
    print("=" * 50)
    print("ETL Pipeline Started")
    print("=" * 50)
    
    # Extract
    transactions = extract('transactions.csv')
    
    if not transactions:
        print("Pipeline failed at extraction stage")
        return
    
    # Transform
    transformed, summary = transform(transactions)
    
    # Load
    success = load(transformed, summary)
    
    print("=" * 50)
    if success:
        print("ETL Pipeline Completed Successfully")
    else:
        print("ETL Pipeline Completed with Errors")
    print("=" * 50)


if __name__ == '__main__':
    main()
