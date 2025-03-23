#!/usr/bin/env python3
import argparse
import json
import os
import sys

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import broker parser factory
from brokers.base_broker import get_broker_parser

def process_csv_file(input_file, broker_type):
    """Process a CSV file using the appropriate broker parser"""
    try:
        # Get the appropriate broker parser
        broker_parser = get_broker_parser(broker_type)
        
        # Process the CSV file
        trades = broker_parser.process_csv(input_file)
        
        return trades
    except Exception as e:
        print(f"Error processing CSV: {str(e)}", file=sys.stderr)
        raise

def main():
    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Process broker CSV files')
    parser.add_argument('input_file', help='The CSV file to process')
    parser.add_argument('--broker', required=True, help='The broker type (fidelity, robinhood, interactive-brokers, charles-schwab)')
    parser.add_argument('--output', help='Output file path (default: input_file_parsed.json)')
    
    args = parser.parse_args()
    
    # Set default output file if not specified
    if not args.output:
        base_name = os.path.splitext(args.input_file)[0]
        args.output = f"{base_name}_parsed.json"
    
    # Process the CSV file
    trades = process_csv_file(args.input_file, args.broker)
    
    # Write the processed data to the output file
    with open(args.output, 'w') as f:
        json.dump(trades, f, indent=2)
    
    # Also print to stdout for piping
    print(json.dumps(trades))

if __name__ == "__main__":
    main()