#!/usr/bin/env python3
from datetime import datetime
from typing import Dict, Any

from .base_broker import BaseBroker


class TradingViewBroker(BaseBroker):
    """TradingView specific CSV processing logic with SQLModel field alignment"""
    
    @property
    def column_mappings(self) -> Dict[str, str]:
        """Map TradingView columns to standardized fields that match SQLModel model"""
        return {
            'Date': 'date',
            'Action': 'side',
            'Symbol': 'symbol',
            'Type': 'description',
            'Quantity': 'quantity',
            'Price': 'price',
            'Fee': 'commission',
            'Value': 'net_proceeds'
        }

    def process_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Process a single row of TradingView data into SQLModel-compatible format"""
        # Skip non-trade transactions
        action = row.get('Action', '')
        if not action or action.upper() in ['DIV', 'DIVIDEND', 'INT', 'INTEREST', 'ADJ']:
            return None
            
        # Create object with SQLModel Trade model field names
        trade = {
            'timestamp': None,               # Will be set from Date
            'date': None,                    # Will be set from Date
            'time': None,                    # Not directly available
            'symbol': None,                  # Will be set from Symbol
            'price': 0.0,                    # Will be set from Price
            'quantity': 0.0,                 # Will be set from Quantity
            'side': None,                    # Will be derived from Action
            'status': 'COMPLETED',           # Default status
            'commission': 0.0,               # Will be set from Fee
            'net_proceeds': 0.0,             # Will be set from Value
            'is_option': False,              # Will be determined from Type/description
            'option_type': None,             # Will be extracted from Type/description
            'strike_price': None,            # Will be extracted from Type/description
            'expiry_date': None,             # Will be extracted from Type/description
            'description': None,             # Will be set from Type
            'broker_type': 'tradingview'     # Hardcoded for TradingView
        }
        
        # Map TradingView fields to SQLModel fields using our mapping
        for tv_col, sqlmodel_field in self.column_mappings.items():
            if tv_col in row and row[tv_col]:
                trade[sqlmodel_field] = row[tv_col]
        
        # Process side (direction)
        if 'side' in trade and trade['side']:
            trade['side'] = self.determine_direction(trade['side'])
        else:
            trade['side'] = 'BUY'  # Default if missing
        
        # Process date and timestamp
        if 'date' in trade and trade['date']:
            date_str = trade['date']
            
            # Parse the date string to a datetime object
            try:
                # Use the base class date parser
                date_obj = self.parse_date(date_str)
                
                # Set the timestamp and formatted date
                trade['timestamp'] = date_obj
                trade['date'] = date_obj.strftime('%Y-%m-%d')
                trade['time'] = date_obj.strftime('%H:%M:%S')
                
            except Exception as e:
                # If parsing fails, use current date
                now = datetime.now()
                trade['timestamp'] = now
                trade['date'] = now.strftime('%Y-%m-%d')
                trade['time'] = now.strftime('%H:%M:%S')
        else:
            # If no date field, use current date
            now = datetime.now()
            trade['timestamp'] = now
            trade['date'] = now.strftime('%Y-%m-%d')
            trade['time'] = now.strftime('%H:%M:%S')
        
        # Process numeric values
        for field in ['price', 'quantity', 'commission', 'net_proceeds']:
            if field in trade and trade[field]:
                trade[field] = self.clean_numeric(trade[field])
            else:
                trade[field] = 0.0
        
        # Process symbol
        if trade.get('symbol'):
            trade['symbol'] = self.extract_base_symbol(trade['symbol'])
        else:
            trade['symbol'] = 'UNKNOWN'
        
        # Process option information from description
        if trade.get('description'):
            option_details = self.extract_option_details(trade['description'], trade.get('symbol'))
            
            trade['is_option'] = option_details['isOption']
            if option_details['isOption']:
                trade['option_type'] = option_details['optionType']
                trade['strike_price'] = option_details['strikePrice']
                
                if option_details['expiryDate']:
                    if isinstance(option_details['expiryDate'], datetime):
                        trade['expiry_date'] = option_details['expiryDate'].strftime('%Y-%m-%d')
                    else:
                        trade['expiry_date'] = option_details['expiryDate']
                
                # For options, multiply price by 100 to get the contract price
                if trade['price'] and trade['price'] < 100:  # Likely a per-share price
                    trade['price'] = trade['price'] * 100
        
        return trade 