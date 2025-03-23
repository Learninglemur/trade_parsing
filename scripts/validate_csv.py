#!/usr/bin/env python3
import csv
import sys
import os
import json
import argparse

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def validate_csv_structure(file_path, broker_type):
    """Validate that the CSV has the required structure for the specified broker"""
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            headers = next(reader, None)
            
            if not headers:
                return {
                    'valid': False,
                    'error': 'CSV file is empty or has no headers'
                }
            
            # Define required fields for each broker type
            required_fields_map = {
                'interactive-brokers': {
                    'Description': 'description',  # matches IB export
                    'Buy/Sell': 'side',
                    'TradeDate': 'date'
                },
                'fidelity': {
                    'Run Date': 'date',
                    'Action': 'side',
                    'Price ($)': 'price'
                },
                'robinhood': {
                    'Activity Date': 'date',
                    'Trans Code': 'side',
                    'Instrument': 'symbol'
                },
                'charles-schwab': {
                    'Date': 'date',
                    'Action': 'side',
                    'Price': 'price'
                }
            }
            
            # Print headers for debugging
            print(f"DEBUG: CSV Headers: {headers}", file=sys.stderr)
            
            # Normalize broker type
            normalized_broker = broker_type.lower().replace(' ', '-')
            
            # Handle aliases
            if normalized_broker == 'td':
                normalized_broker = 'fidelity'
            elif normalized_broker == 'schwab':
                normalized_broker = 'charles-schwab'
            elif normalized_broker == 'ib':
                normalized_broker = 'interactive-brokers'
            elif normalized_broker == 'td-ameritrade':
                normalized_broker = 'fidelity'
            
            if normalized_broker not in required_fields_map:
                return {
                    'valid': False,
                    'error': f'Unsupported broker type: {broker_type}'
                }
            
            # Check if there's at least one data row
            first_row = next(reader, None)
            if not first_row:
                return {
                    'valid': False,
                    'error': 'CSV file has headers but no data rows'
                }
            
            # For TD Ameritrade, be more flexible with column names
            if normalized_broker == 'td-ameritrade':
                # Check for alternative column names
                date_columns = ['Run Date', 'Date', 'Trade Date', 'Activity Date']
                side_columns = ['Action', 'Type', 'Transaction Type', 'Trans Code']
                price_columns = ['Price ($)', 'Price', 'Trade Price']
                
                # Check if at least one of each type is present
                has_date = any(col in headers for col in date_columns)
                has_side = any(col in headers for col in side_columns)
                has_price = any(col in headers for col in price_columns)
                
                if not (has_date and has_side):
                    missing = []
                    if not has_date:
                        missing.append("date column (Run Date, Date, Trade Date, or Activity Date)")
                    if not has_side:
                        missing.append("side column (Action, Type, Transaction Type, or Trans Code)")
                    
                    return {
                        'valid': False,
                        'error': f'Missing essential fields: {", ".join(missing)}'
                    }
                
                # If we have the minimum required fields, consider it valid
                return {
                    'valid': True
                }
            
            # For other brokers, check required fields but be more lenient
            required_fields = required_fields_map[normalized_broker]
            
            # Check if all required fields are present
            missing_fields = []
            for field in required_fields:
                if field not in headers:
                    missing_fields.append(field)
            
            # For most brokers, we need at least a date and side field
            if missing_fields:
                # Check if we have the minimum required fields (date and side)
                has_date = any(field in headers for field, mapped in required_fields.items() if mapped == 'date')
                has_side = any(field in headers for field, mapped in required_fields.items() if mapped == 'side')
                
                if not (has_date and has_side):
                    return {
                        'valid': False,
                        'error': f'Missing essential fields: {", ".join(missing_fields)}'
                    }
            
            return {
                'valid': True
            }
            
    except Exception as e:
        return {
            'valid': False,
            'error': f'Error validating CSV: {str(e)}'
        }

def main():
    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Validate broker CSV files')
    parser.add_argument('file_path', help='The CSV file to validate')
    parser.add_argument('broker_type', help='The broker type (Robinhood, TD Ameritrade, Interactive Brokers, Schwab)')
    
    args = parser.parse_args()
    
    result = validate_csv_structure(args.file_path, args.broker_type)
    print(json.dumps(result))

if __name__ == "__main__":
    main()