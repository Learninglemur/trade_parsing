#!/usr/bin/env python3
from datetime import datetime
import re
import sys
from typing import Dict, Any, Optional

from .base_broker import BaseBroker
from .symbol_enhancer import lookup_stock_symbol, needs_enhancement, extract_option_details as enhanced_extract


class RobinhoodBroker(BaseBroker):
    """Robinhood specific CSV processing logic with SQLModel field alignment"""
    
    @property
    def column_mappings(self) -> Dict[str, str]:
        """Map Robinhood columns to standardized fields that match SQLModel model"""
        return {
            # Primary column names - actual Robinhood format
            'Activity Date': 'date',           # Maps to date
            'Process Date': 'process_date',    # Not directly used in model
            'Settle Date': 'settle_date',      # Not directly used in model
            'Instrument': 'symbol',            # Maps to symbol
            'Description': 'description',      # Maps to description
            'Trans Code': 'side',              # Maps to side after conversion
            'Quantity': 'quantity',            # Maps to quantity
            'Price': 'price',                  # Maps to price
            'Amount': 'net_proceeds',          # Maps to net_proceeds
            
            # Alternative column names
            'Date': 'date',                    # Alternative date column
            'Trade Date': 'date',              # Alternative date column
            'Symbol': 'symbol',                # Alternative symbol column
            'Action': 'side',                  # Alternative side column
            'Type': 'side',                    # Alternative side column
            'Transaction Type': 'side',        # Alternative side column
            'Side': 'side',                    # Alternative side column
            'Shares': 'quantity',              # Alternative quantity column
            'Trade Price': 'price',            # Alternative price column
            'Qty/Amt': 'quantity',             # Alternative quantity column
            'Net Amount': 'net_proceeds',      # Alternative net_proceeds column
            'Expiry Date': 'expiry_date',      # Direct mapping for expiry date
            'Option Type': 'option_type',      # Direct mapping for option type
            'Strike Price': 'strike_price'     # Direct mapping for strike price
        }
    
    @property
    def use_symbol_enhancement(self) -> bool:
        """Whether this broker should use symbol enhancement"""
        return True
    
    def parse_transaction_code(self, code: str) -> str:
        """Convert Robinhood transaction codes to BUY/SELL"""
        if not code:
            return 'BUY'  # Default to BUY if missing
            
        code = code.upper()
        buy_codes = ['BTO', 'BTC', 'BUY', 'B']
        sell_codes = ['STO', 'STC', 'SELL', 'S']
        
        # Treat "Option Expiration" and "OEXP" as SELL
        if code == 'OPTION EXPIRATION' or 'EXPIRATION' in code or code == 'OEXP':
            return 'SELL'
            
        if any(buy_term in code for buy_term in buy_codes):
            return 'BUY'
        elif any(sell_term in code for sell_term in sell_codes):
            return 'SELL'
        else:
            # Default to BUY for unrecognized values
            print(f"Warning: Unrecognized side value: {code}, defaulting to BUY", file=sys.stderr)
            return 'BUY'
    
    def extract_option_details(self, description: str, symbol: Optional[str] = None, 
                              extra_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Extract option details from Robinhood description format"""
        # Get trade date from extra_data if available
        trade_date = None
        if extra_data:
            if 'trade_date' in extra_data:
                trade_date = extra_data['trade_date']
            elif 'date' in extra_data:
                trade_date = extra_data['date']
        
        # Use the more advanced implementation from symbol_enhancer
        result = enhanced_extract(description, symbol, trade_date)
        
        # Return result directly since property names match
        return result
    
    def process_symbol(self, symbol: str, description: Optional[str] = None) -> str:
        """Process and enhance the symbol if needed"""
        if not self.use_symbol_enhancement:
            return symbol
            
        if needs_enhancement(symbol):
            return lookup_stock_symbol(symbol, description)
            
        return symbol
    
    def process_row(self, row: Dict[str, str], extra_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Process a single row of Robinhood data into SQLModel-compatible format"""
        # Skip non-trade transactions (be flexible with column names)
        trans_code = ''
        for col in ['Trans Code', 'Type', 'Transaction Type', 'Action', 'Side']:
            if col in row and row[col]:
                trans_code = row[col]
                break
                
        print(f"Transaction code: {trans_code}")
                
        if not trans_code or trans_code.upper() in ['INT', 'ACH', 'RTP', 'DIV', 'CDIV']:
            print(f"Skipping row - transaction code missing or is non-trade: {trans_code}")
            return None
            
        # Create object with SQLModel Trade model field names
        trade = {
            'timestamp': None,               # Will be generated from date
            'date': None,                    # Will be set from Activity Date
            'time': None,                    # Not directly available
            'symbol': None,                  # Will be set from Instrument
            'price': 0.0,                    # Will be set from Price
            'quantity': 0.0,                 # Will be set from Quantity
            'side': None,                    # Will be derived from Trans Code
            'status': 'COMPLETED',           # Default status
            'commission': 0.0,               # Not directly available
            'net_proceeds': 0.0,             # Will be set from Amount
            'is_option': False,              # Will be determined from Description
            'option_type': None,             # Will be extracted from Description
            'strike_price': None,            # Will be extracted from Description
            'expiry_date': None,             # Will be extracted from Description
            'description': None,             # Will be set from Description
            'broker_type': 'robinhood',      # Hardcoded for Robinhood
            'dte': None                      # Will be calculated based on trade date and expiry date
        }
        
        # Map Robinhood fields to SQLModel fields using our mapping
        for robinhood_col, sqlmodel_field in self.column_mappings.items():
            if robinhood_col in row and row[robinhood_col]:
                trade[sqlmodel_field] = row[robinhood_col]
        
        # Process side (direction) from transaction code
        if 'side' in trade and trade['side']:
            trade['side'] = self.parse_transaction_code(trade['side'])
        else:
            trade['side'] = 'BUY'  # Default if missing
        
        # Process date and timestamp
        activity_date = None
        
        # First try to use Activity Date
        if 'Activity Date' in row and row['Activity Date']:
            activity_date = row['Activity Date']
        # Then fall back to other date fields
        elif 'date' in trade and trade['date']:
            activity_date = trade['date']
            
        if activity_date:
            # Parse the date string to a datetime object
            try:
                # Check for mm/dd/yyyy format (common in Robinhood)
                if '/' in activity_date:
                    date_obj = datetime.strptime(activity_date, '%m/%d/%Y')
                else:
                    # Use the base class date parser for other formats
                    date_obj = self.parse_date(activity_date)
                
                # Set the timestamp and formatted date
                trade['timestamp'] = date_obj
                trade['date'] = date_obj.strftime('%Y-%m-%d')
                trade['time'] = date_obj.strftime('%H:%M:%S')
                
                # Save the original date for option DTE calculation
                if extra_data is None:
                    extra_data = {}
                extra_data['trade_date'] = date_obj
                
            except Exception as e:
                # If parsing fails, use current date
                print(f"Warning: Could not parse date '{activity_date}': {str(e)}", file=sys.stderr)
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
        
        # Make net_proceeds negative for buys, positive for sells
        if trade['side'] == 'BUY' and trade['net_proceeds'] > 0:
            trade['net_proceeds'] = -trade['net_proceeds']
        
        # Special handling for option expirations
        if 'EXPIRATION' in trans_code.upper() or trans_code.upper() == 'OEXP':
            trade['side'] = 'SELL'  # Explicitly mark as a sell
            
            # Set net_proceeds for expirations if it's 0 or missing
            # This ensures expirations are clearly treated as sell orders
            if trade['net_proceeds'] == 0:
                # Use price * quantity as an estimated value
                trade['net_proceeds'] = trade['price'] * trade['quantity']
        
        # If we don't have a symbol, try to extract it from description
        if not trade.get('symbol') and trade.get('description'):
            trade['symbol'] = self.extract_base_symbol(trade['description'])
        elif not trade.get('symbol'):
            trade['symbol'] = 'UNKNOWN'
        
        # Process option information from description
        if trade.get('description'):
            print(f"Extracting option details from description: {trade['description']}")
            
            # Set up trade_date for DTE calculation
            trade_date = None
            if 'timestamp' in trade and isinstance(trade['timestamp'], datetime):
                trade_date = trade['timestamp']
            
            # Prepare option_extra_data with the trade date
            option_extra_data = {}
            if trade_date:
                option_extra_data['trade_date'] = trade_date
            elif extra_data and 'trade_date' in extra_data:
                option_extra_data['trade_date'] = extra_data['trade_date']
                trade_date = extra_data['trade_date']
                
            # Extract option details from description
            option_details = self.extract_option_details(trade['description'], trade.get('symbol'), option_extra_data)
            print(f"Extracted option details: {option_details}")
            
            # Always set is_option flag from the result
            trade['is_option'] = option_details['is_option']
            
            # Process option-specific fields
            if option_details['is_option']:
                # Set option type and strike price
                trade['option_type'] = option_details['option_type']
                trade['strike_price'] = option_details['strike_price']
                
                # Handle expiry date - ensure it's always properly set
                if option_details['expiry_date']:
                    # Convert expiry_date to string format if it's a datetime
                    if isinstance(option_details['expiry_date'], datetime):
                        trade['expiry_date'] = option_details['expiry_date'].strftime('%Y-%m-%d')
                    else:
                        trade['expiry_date'] = option_details['expiry_date']
                    
                    # Set DTE if available in option_details
                    if 'dte' in option_details and option_details['dte'] is not None:
                        trade['dte'] = option_details['dte']
                    # Calculate DTE if not in option_details but we have trade_date
                    elif trade_date and option_details['expiry_date']:
                        from brokers.symbol_enhancer import calculate_dte
                        expiry_date = option_details['expiry_date']
                        if isinstance(expiry_date, str):
                            try:
                                expiry_date = datetime.strptime(expiry_date, '%Y-%m-%d')
                            except ValueError:
                                # If date format is incorrect, skip DTE calculation
                                pass
                        if isinstance(expiry_date, datetime):
                            trade['dte'] = calculate_dte(trade_date, expiry_date)
                
                # Try a fallback for expiry date for option expirations if not set
                if not trade.get('expiry_date') and ('EXPIRATION' in trade['description'].upper() or 'OEXP' in trade['description'].upper() or trans_code.upper() == 'OEXP'):
                    # Try various date patterns
                    date_patterns = [
                        r'(\d{1,2})/(\d{1,2})/(\d{4})',  # mm/dd/yyyy
                        r'(\d{4})-(\d{1,2})-(\d{1,2})',  # yyyy-mm-dd
                    ]
                    
                    for pattern in date_patterns:
                        date_match = re.search(pattern, trade['description'])
                        if date_match:
                            try:
                                if pattern == r'(\d{1,2})/(\d{1,2})/(\d{4})':
                                    month = int(date_match.group(1))
                                    day = int(date_match.group(2))
                                    year = int(date_match.group(3))
                                else:  # yyyy-mm-dd
                                    year = int(date_match.group(1))
                                    month = int(date_match.group(2))
                                    day = int(date_match.group(3))
                                
                                expiry_date = datetime(year, month, day)
                                trade['expiry_date'] = expiry_date.strftime('%Y-%m-%d')
                                
                                # Calculate DTE if we have a trade date
                                if trade_date:
                                    from brokers.symbol_enhancer import calculate_dte
                                    trade['dte'] = calculate_dte(trade_date, expiry_date)
                                break  # Found a valid date, exit the loop
                            except ValueError:
                                continue  # Try next pattern
                
                # For options, multiply price by 100 to get the contract price if it seems low
                if trade['price'] and trade['price'] < 100:  # Likely a per-share price
                    trade['price'] = trade['price'] * 100
        
        return trade