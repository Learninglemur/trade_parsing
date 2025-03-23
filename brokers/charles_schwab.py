#!/usr/bin/env python3
from datetime import datetime
import re
from typing import Dict, Any, Optional

from .base_broker import BaseBroker


class CharlesSchwabBroker(BaseBroker):
    """Charles Schwab specific CSV processing logic with SQLModel field alignment"""
    
    @property
    def column_mappings(self) -> Dict[str, str]:
        """Map Schwab columns to standardized fields that match SQLModel model"""
        return {
            'Date': 'date',                # Maps to date
            'Action': 'side',              # Maps to side
            'Quantity': 'quantity',        # Maps to quantity
            'Symbol': 'symbol',            # Maps to symbol
            'Description': 'description',  # Maps to description
            'Price': 'price',              # Maps to price
            'Amount': 'net_proceeds',      # Maps to net_proceeds
            'Comm/Fees': 'commission',     # Maps to commission
            'Fees & Comm': 'commission',   # Alternative commission field
            'Strike': 'strike_price',      # Maps to strike_price (from trading screen)
            'Last': 'last_price',          # Additional info, not in model directly
            'Bid': 'bid_price',            # Additional info, not in model directly
            'Ask': 'ask_price'             # Additional info, not in model directly
        }
    
    def format_option_symbol(self, underlying: str, expiry: Any, strike: float, option_type: str) -> Optional[str]:
        """Format option symbol in standardized format"""
        if not all([underlying, expiry, strike, option_type]):
            return None
            
        # Format strike price with 2 decimal places
        strike_formatted = "{:.2f}".format(float(strike))
        
        # Format expiry date in MM/DD/YYYY format
        if isinstance(expiry, datetime):
            expiry_formatted = expiry.strftime('%m/%d/%Y')
        else:
            expiry_formatted = expiry
            
        # Format option type as "C" or "P"
        opt_type = "C" if option_type.upper() == "CALL" else "P"
        
        # Return formatted symbol: "OEX 12/19/2009 495.00 C"
        return f"{underlying} {expiry_formatted} {strike_formatted} {opt_type}"
    
    def extract_option_details(self, description: str, symbol: Optional[str] = None,
                              extra_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Extract option details from Schwab description and symbol"""
        if not description and not symbol:
            return {
                'isOption': False,
                'optionType': None,
                'strikePrice': None,
                'expiryDate': None
            }
            
        details = {
            'isOption': False,
            'optionType': None,
            'strikePrice': None,
            'expiryDate': None
        }
            
        # Handle format seen in Image 2: "OEX 12/19/2009 495.00 C"
        schwab_match = re.search(r'^([A-Z]+)\s+(\d+/\d+/\d+)\s+(\d+(?:\.\d+)?)\s+([CP])$', symbol if symbol else description)
        if schwab_match:
            details['isOption'] = True
            details['optionType'] = 'CALL' if schwab_match.group(4) == 'C' else 'PUT'
            details['strikePrice'] = float(schwab_match.group(3))
            try:
                details['expiryDate'] = datetime.strptime(schwab_match.group(2), '%m/%d/%Y')
            except ValueError:
                pass
                
        # Handle format seen in Image 1: "ORBIT INTL C"
        elif description and (description.upper().endswith(' C') or description.upper().endswith(' P')):
            details['isOption'] = True
            details['optionType'] = 'CALL' if description.upper().endswith(' C') else 'PUT'
            
            # For older Schwab format, we might need additional data to get strike/expiry
            if extra_data:
                if 'strike_price' in extra_data:
                    details['strikePrice'] = extra_data['strike_price']
                elif 'strikePrice' in extra_data:  # Handle legacy camelCase
                    details['strikePrice'] = extra_data['strikePrice']
                    
                if 'expiry_date' in extra_data:
                    details['expiryDate'] = extra_data['expiry_date']
                elif 'expiryDate' in extra_data:  # Handle legacy camelCase
                    details['expiryDate'] = extra_data['expiryDate']
            
        return details
    
    def process_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Process a single row of Schwab data into SQLModel-compatible format"""
        # Create object with SQLModel Trade model field names
        trade = {
            'timestamp': None,                 # Will be generated from date
            'date': None,                      # Will be set from Date
            'time': None,                      # Not directly available in basic view
            'symbol': None,                    # Will be set from Symbol
            'price': 0.0,                      # Will be set from Price
            'quantity': 0.0,                   # Will be set from Quantity
            'side': None,                      # Will be set from Action
            'status': 'COMPLETED',             # Default status
            'commission': 0.0,                 # Will be set from Comm/Fees
            'net_proceeds': 0.0,               # Will be set from Amount
            'is_option': False,                # Will be determined from Description
            'option_type': None,               # Will be extracted from Description
            'strike_price': None,              # Will be set from Strike or extracted
            'expiry_date': None,               # Will be extracted if available
            'description': None,               # Will be set from Description
            'broker_type': 'charles-schwab'    # Hardcoded for Schwab
        }
        
        # Skip non-trade rows
        action = row.get('Action', '')
        if not action or action not in ['Buy', 'Sell', 'Buy to Open', 'Sell to Open', 'Buy to Close', 'Sell to Close']:
            return None
            
        # Map Schwab fields to SQLModel fields using our mapping
        for schwab_col, sqlmodel_field in self.column_mappings.items():
            if schwab_col in row and row[schwab_col]:
                trade[sqlmodel_field] = row[schwab_col]
        
        # Process symbol
        if 'symbol' not in trade or not trade['symbol']:
            if 'description' in trade:
                trade['symbol'] = self.extract_base_symbol(trade['description'])
        
        # Process side (direction)
        if 'side' in trade:
            # Standardize to "BUY" or "SELL"
            action = trade['side']
            if action in ['Buy', 'Buy to Open', 'Buy to Close']:
                trade['side'] = 'BUY'
            elif action in ['Sell', 'Sell to Open', 'Sell to Close']:
                trade['side'] = 'SELL'
        
        # Process date and timestamp
        if 'date' in trade and trade['date']:
            try:
                # Use the base class date parser
                date_obj = self.parse_date(trade['date'])
                
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
        for field in ['price', 'quantity', 'commission', 'net_proceeds', 'strike_price']:
            if field in trade and trade[field]:
                trade[field] = self.clean_numeric(trade[field])
            else:
                trade[field] = 0.0 if field != 'strike_price' else None
        
        # Ensure net_proceeds sign matches the side (negative for buys, positive for sells)
        if trade['side'] == 'BUY' and trade['net_proceeds'] > 0:
            trade['net_proceeds'] = -trade['net_proceeds']
        
        # Process option information
        extra_data = {
            'strike_price': trade.get('strike_price')
        }
        
        option_info = self.extract_option_details(
            trade.get('description', ''), 
            trade.get('symbol', ''),
            extra_data
        )
        
        if option_info['isOption']:
            trade['is_option'] = True
            trade['option_type'] = option_info['optionType']
            
            # Use strike_price if available from option_info or Strike column
            if option_info['strikePrice']:
                trade['strike_price'] = option_info['strikePrice']
                
            # Use expiry_date if available
            if option_info['expiryDate']:
                if isinstance(option_info['expiryDate'], datetime):
                    trade['expiry_date'] = option_info['expiryDate'].strftime('%Y-%m-%d')
                else:
                    trade['expiry_date'] = option_info['expiryDate']
                
            # For options, multiply price by 100 to get the contract price
            if trade['price'] and trade['price'] < 100:  # Likely a per-share price
                trade['price'] = trade['price'] * 100
        
        return trade