#!/usr/bin/env python3
from datetime import datetime, timedelta
import re
import sys
from typing import Dict, Any, Optional

from .base_broker import BaseBroker
from .symbol_enhancer import lookup_stock_symbol, needs_enhancement, extract_option_details as symbol_extract_option_details


class InteractiveBrokersBroker(BaseBroker):
    """Interactive Brokers specific CSV processing logic with SQLModel field alignment"""
    
    @property
    def column_mappings(self) -> Dict[str, str]:
        """Map IBKR columns to standardized fields that match SQLModel model"""
        return {
            # Primary column names
            'Description': 'description',      # Maps to description
            'Conid': 'conid',                  # Internal IB identifier
            'SecurityID': 'security_id',       # Security identifier
            'Symbol': 'symbol',                # Maps to symbol
            'TradeDate': 'date',               # Maps to date
            'TradeTime': 'time',               # Maps to time
            'DateTime': 'timestamp',           # Maps to timestamp
            'Buy/Sell': 'side',                # Maps to side
            'Quantity': 'quantity',            # Maps to quantity
            'NetCash': 'net_proceeds',         # Maps to net_proceeds
            'TradePrice': 'price',             # Maps to price
            'Commission': 'commission',        # Maps to commission
            'Put/Call': 'option_type',         # Maps to option type
            'Strike': 'strike_price',          # Maps to strike price
            'Expiry': 'expiry_date',           # Maps to expiry date
            'IBCommission': 'commission',      # Maps to commission
            
            # Alternative column names
            'Date': 'date',                    # Alternative date column
            'Time': 'time',                    # Alternative time column
            'Action': 'side',                  # Alternative side column
            'Type': 'side',                    # Alternative side column
            'Transaction Type': 'side',        # Alternative side column
            'Shares': 'quantity',              # Alternative quantity column
            'Price': 'price',                  # Alternative price column
            'Amount': 'net_proceeds',          # Alternative net_proceeds column
            'Net Amount': 'net_proceeds'       # Alternative net_proceeds column
        }
    
    @property
    def use_symbol_enhancement(self) -> bool:
        """Whether this broker should use symbol enhancement"""
        return True
    
    def extract_ticker_only(self, symbol: str) -> str:
        """Extract just the ticker symbol without any exchange or class identifiers"""
        if not symbol:
            return 'UNKNOWN'
            
        # Remove any exchange identifiers (e.g., AAPL:NASDAQ -> AAPL)
        if ':' in symbol:
            symbol = symbol.split(':')[0]
            
        # Remove any class identifiers (e.g., BRK.B -> BRK)
        if '.' in symbol:
            symbol = symbol.split('.')[0]
            
        # Remove any option identifiers (e.g., AAPL1234C -> AAPL)
        match = re.match(r'^([A-Z]+)', symbol)
        if match:
            return match.group(1)
            
        return symbol
    
    def extract_option_details(self, description: str, symbol: Optional[str] = None, 
                              extra_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Extract option details from Interactive Brokers description format using enhanced pattern matching"""
        if not description:
            return {
                'isOption': False,
                'optionType': None,
                'strikePrice': None,
                'expiryDate': None,
                'daysToExpiry': None
            }
        
        # Process directly from extra_data if available
        trade_date = None
        if extra_data:
            if extra_data.get('transaction_date'):
                try:
                    trade_date = extra_data['transaction_date']
                    if isinstance(trade_date, str):
                        trade_date = datetime.strptime(trade_date, '%Y-%m-%d')
                except (ValueError, TypeError):
                    pass
                    
        # Use the central extraction function from symbol_enhancer
        result = symbol_extract_option_details(description, symbol, trade_date)
        
        # Convert to the format expected by this class
        return {
            'isOption': result['is_option'],
            'optionType': result['option_type'],
            'strikePrice': result['strike_price'],
            'expiryDate': result['expiry_date'],
            'daysToExpiry': result['dte']
        }
    
    def process_symbol(self, symbol: str, description: Optional[str] = None) -> str:
        """Process and enhance the symbol if needed"""
        if not self.use_symbol_enhancement:
            return symbol
            
        if needs_enhancement(symbol):
            return lookup_stock_symbol(symbol, description)
            
        return symbol
    
    def process_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Process a single row of Interactive Brokers data into SQLModel-compatible format"""
        # Skip non-trade transactions
        action = None
        for col in ['Buy/Sell', 'Action', 'Type', 'Transaction Type']:
            if col in row and row[col]:
                action = row[col]
                break
                
        if not action or action.upper() in ['DIV', 'DIVIDEND', 'INT', 'INTEREST', 'ADJ', 'ADJUSTMENT']:
            return None
            
        # Create object with SQLModel Trade model field names
        trade = {
            'timestamp': None,               # Will be set from DateTime or generated from date+time
            'date': None,                    # Will be set from TradeDate
            'time': None,                    # Will be set from TradeTime
            'symbol': None,                  # Will be set from Symbol
            'price': 0.0,                    # Will be set from TradePrice or calculated
            'quantity': 0.0,                 # Will be set from Quantity
            'side': None,                    # Will be set from Buy/Sell
            'status': 'COMPLETED',           # Default status
            'commission': 0.0,               # Not directly available
            'net_proceeds': 0.0,             # Will be set from NetCash
            'is_option': False,              # Will be determined from Description
            'option_type': None,             # Will be extracted from Description
            'strike_price': None,            # Will be extracted from Description
            'expiry_date': None,             # Will be extracted from Description
            'description': None,             # Will be set from Description
            'broker_type': 'interactive-brokers'  # Hardcoded for Interactive Brokers
        }
        
        # Map IBKR fields to SQLModel fields using our mapping
        for ibkr_col, sqlmodel_field in self.column_mappings.items():
            if ibkr_col in row and row[ibkr_col]:
                trade[sqlmodel_field] = row[ibkr_col]
        
        # Process side (direction)
        if 'side' in trade and trade['side']:
            side = trade['side'].upper()
            if any(buy_term in side for buy_term in ['BUY', 'B', 'BTO', 'BTC']):
                trade['side'] = 'BUY'
            elif any(sell_term in side for sell_term in ['SELL', 'S', 'STO', 'STC']):
                trade['side'] = 'SELL'
            else:
                # Default to BUY for unrecognized values
                print(f"Warning: Unrecognized side value: {side}, defaulting to BUY", file=sys.stderr)
                trade['side'] = 'BUY'
        else:
            # Default if missing
            trade['side'] = 'BUY'
        
        # Process date and time
        # If we have a timestamp field, use that
        if trade.get('timestamp'):
            # Try to parse the timestamp
            try:
                # IB format could be various formats
                timestamp = str(trade['timestamp'])
                
                # Clean up the timestamp
                timestamp = timestamp.strip()
                
                # Handle AM/PM format: 2024-07-09 9:39:23 AM
                am_pm_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2}):(\d{2})\s+(AM|PM)', timestamp)
                if am_pm_match:
                    date_str = am_pm_match.group(1)
                    hour = int(am_pm_match.group(2))
                    minute = int(am_pm_match.group(3))
                    second = int(am_pm_match.group(4))
                    am_pm = am_pm_match.group(5).upper()
                    
                    # Convert to 24-hour format
                    if am_pm == 'PM' and hour < 12:
                        hour += 12
                    elif am_pm == 'AM' and hour == 12:
                        hour = 0
                        
                    time_str = f"{hour:02d}:{minute:02d}:{second:02d}"
                    timestamp = f"{date_str} {time_str}"
                    dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                # Handle other AM/PM format without seconds: 2024-07-09 9:39 AM
                elif re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2})\s+(AM|PM)', timestamp):
                    match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2})\s+(AM|PM)', timestamp)
                    date_str = match.group(1)
                    hour = int(match.group(2))
                    minute = int(match.group(3))
                    am_pm = match.group(4).upper()
                    
                    # Convert to 24-hour format
                    if am_pm == 'PM' and hour < 12:
                        hour += 12
                    elif am_pm == 'AM' and hour == 12:
                        hour = 0
                        
                    time_str = f"{hour:02d}:{minute:02d}:00"
                    timestamp = f"{date_str} {time_str}"
                    dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                elif 'T' in timestamp:
                    # ISO format
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                elif ' ' in timestamp:
                    # Try different space-separated formats
                    formats = [
                        '%Y-%m-%d %H:%M:%S',
                        '%Y-%m-%d %H:%M',
                        '%m/%d/%Y %H:%M:%S',
                        '%m/%d/%Y %H:%M',
                        '%d/%m/%Y %H:%M:%S',
                        '%d/%m/%Y %H:%M'
                    ]
                    
                    dt = None
                    for fmt in formats:
                        try:
                            dt = datetime.strptime(timestamp, fmt)
                            break
                        except ValueError:
                            continue
                            
                    if dt is None:
                        # Fallback to just parsing the date
                        date_part = timestamp.split()[0]
                        dt = datetime.strptime(date_part, '%Y-%m-%d')
                else:
                    # Just date
                    formats = ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']
                    dt = None
                    for fmt in formats:
                        try:
                            dt = datetime.strptime(timestamp, fmt)
                            break
                        except ValueError:
                            continue
                            
                    if dt is None:
                        # Last attempt - generic fallback
                        dt = datetime.now()
                        print(f"Warning: Could not parse timestamp '{timestamp}', using current time", file=sys.stderr)
                
                trade['timestamp'] = dt
                trade['date'] = dt.strftime('%Y-%m-%d')
                trade['time'] = dt.strftime('%H:%M:%S')
            except ValueError as e:
                print(f"Warning: Could not parse timestamp '{trade['timestamp']}': {e}", file=sys.stderr)
                trade['timestamp'] = datetime.now()
                
        # If we only have a date field but no timestamp
        if not trade.get('timestamp') and trade.get('date'):
            date_str = str(trade['date']).strip()
            time_str = str(trade.get('time', '00:00:00')).strip()
            
            try:
                # Try to parse date first
                date_formats = ['%Y-%m-%d', '%Y%m%d', '%m/%d/%Y', '%d/%m/%Y', '%d-%m-%Y']
                date_obj = None
                
                for fmt in date_formats:
                    try:
                        date_obj = datetime.strptime(date_str, fmt)
                        break
                    except ValueError:
                        continue
                
                if not date_obj:
                    # Use the base class date parser as a fallback
                    date_obj = self.parse_date(date_str)
                
                trade['date'] = date_obj.strftime('%Y-%m-%d')
                
                # Try to parse time if available
                if time_str:
                    # Handle AM/PM format
                    am_pm_match = re.search(r'(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)', time_str, re.IGNORECASE)
                    if am_pm_match:
                        hour = int(am_pm_match.group(1))
                        minute = int(am_pm_match.group(2))
                        second = int(am_pm_match.group(3)) if am_pm_match.group(3) else 0
                        am_pm = am_pm_match.group(4).upper()
                        
                        # Convert to 24-hour format
                        if am_pm == 'PM' and hour < 12:
                            hour += 12
                        elif am_pm == 'AM' and hour == 12:
                            hour = 0
                    else:
                        try:
                            # Handle various time formats
                            if ':' in time_str:
                                # HH:MM:SS or HH:MM
                                time_parts = time_str.split(':')
                                hours = int(time_parts[0])
                                minutes = int(time_parts[1])
                                seconds = int(time_parts[2]) if len(time_parts) > 2 else 0
                            else:
                                # HHMMSS or HHMM
                                time_str = time_str.strip()
                                if len(time_str) >= 6:
                                    hours = int(time_str[:2])
                                    minutes = int(time_str[2:4])
                                    seconds = int(time_str[4:6])
                                elif len(time_str) >= 4:
                                    hours = int(time_str[:2])
                                    minutes = int(time_str[2:4])
                                    seconds = 0
                                else:
                                    hours = int(time_str)
                                    minutes = 0
                                    seconds = 0
                            
                            # Validate time values
                            hour = min(23, max(0, hours))
                            minute = min(59, max(0, minutes))
                            second = min(59, max(0, seconds))
                        except (ValueError, IndexError) as e:
                            print(f"Warning: Could not parse time '{time_str}': {str(e)}", file=sys.stderr)
                            hour = 0
                            minute = 0
                            second = 0
                    
                    # Combine with date
                    trade['timestamp'] = datetime(
                        date_obj.year, date_obj.month, date_obj.day,
                        hour, minute, second
                    )
                    trade['time'] = f"{hour:02d}:{minute:02d}:{second:02d}"
                else:
                    trade['time'] = '00:00:00'
                    trade['timestamp'] = date_obj
                
            except Exception as e:
                # If parsing fails, use current date
                now = datetime.now()
                trade['timestamp'] = now
                trade['date'] = now.strftime('%Y-%m-%d')
                trade['time'] = now.strftime('%H:%M:%S')
                print(f"Warning: Could not parse date '{date_str}': {str(e)}", file=sys.stderr)
        
        # If we have neither date nor timestamp, use current date
        if not trade.get('timestamp'):
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
            
        # Ensure quantity is positive
        trade['quantity'] = abs(trade['quantity'])
        
        # Clean up the symbol - extract just the ticker
        if trade.get('symbol'):
            trade['symbol'] = self.extract_ticker_only(trade['symbol'])
        
        # If we don't have a symbol, try to extract it from description
        if not trade.get('symbol') and trade.get('description'):
            trade['symbol'] = self.extract_base_symbol(trade['description'])
        elif not trade.get('symbol'):
            trade['symbol'] = 'UNKNOWN'
        
        # Process option information
        if row.get('Put/Call') or row.get('Strike') or row.get('Expiry'):
            extra_data = {
                'option_type': row.get('Put/Call'),
                'strike_price': row.get('Strike'),
                'expiry_date': row.get('Expiry'),
                'transaction_date': trade.get('date')  # Pass transaction date for days to expiry calculation
            }
            
            # If Put/Call column has C or CALL, mark as option
            if row.get('Put/Call') and row.get('Put/Call').upper() in ['C', 'CALL']:
                trade['is_option'] = True
                trade['option_type'] = 'CALL'
            
            # If Put/Call column has P or PUT, mark as option    
            elif row.get('Put/Call') and row.get('Put/Call').upper() in ['P', 'PUT']:
                trade['is_option'] = True
                trade['option_type'] = 'PUT'
                
            # Process option information from description with the column data
            if trade.get('description'):
                option_info = self.extract_option_details(trade['description'], trade.get('symbol'), extra_data)
                
                # Always update option fields based on detection result
                trade['is_option'] = trade.get('is_option', False) or option_info['isOption']
                
                if option_info['optionType']:
                    trade['option_type'] = option_info['optionType']
                    
                if option_info['strikePrice']:
                    trade['strike_price'] = option_info['strikePrice']
                    
                if option_info['expiryDate']:
                    if isinstance(option_info['expiryDate'], datetime):
                        trade['expiry_date'] = option_info['expiryDate']
                    else:
                        # Try to parse the date string
                        try:
                            trade['expiry_date'] = datetime.strptime(option_info['expiryDate'], '%Y-%m-%d')
                        except (ValueError, TypeError):
                            # Keep as is if it can't be parsed
                            trade['expiry_date'] = option_info['expiryDate']
                
                # Add days to expiry if available
                if option_info['daysToExpiry'] is not None:
                    trade['dte'] = option_info['daysToExpiry']
        else:
            # No direct option columns, rely only on description
            if trade.get('description'):
                # Pass transaction date for days to expiry calculation
                option_info = self.extract_option_details(
                    trade['description'], 
                    trade.get('symbol'),
                    {'transaction_date': trade.get('date')}
                )
                
                # Always update options fields if detected
                trade['is_option'] = option_info['isOption']
                if option_info['isOption']:
                    trade['option_type'] = option_info['optionType']
                    trade['strike_price'] = option_info['strikePrice']
                    
                    if option_info['expiryDate']:
                        if isinstance(option_info['expiryDate'], datetime):
                            trade['expiry_date'] = option_info['expiryDate']
                        else:
                            # Try to parse the date string
                            try:
                                trade['expiry_date'] = datetime.strptime(option_info['expiryDate'], '%Y-%m-%d')
                            except (ValueError, TypeError):
                                # Keep as is if it can't be parsed
                                trade['expiry_date'] = option_info['expiryDate']
                    
                    # Add days to expiry if available
                    if option_info['daysToExpiry'] is not None:
                        trade['dte'] = option_info['daysToExpiry']
                    else:
                        # Calculate DTE if we have expiry date and trade date
                        if trade.get('expiry_date') and trade.get('date'):
                            try:
                                expiry_date = trade['expiry_date']
                                trade_date = datetime.strptime(trade['date'], '%Y-%m-%d')
                                trade['dte'] = max(0, (expiry_date - trade_date).days)
                            except (ValueError, TypeError, AttributeError):
                                pass
        
        # For options, multiply price by 100 to get the contract price if it's a per-share price
        if trade['is_option'] and trade['price'] and trade['price'] < 100:
            trade['price'] = trade['price'] * 100
        
        # If price is missing, calculate from net_proceeds and quantity
        if not trade['price'] and trade['quantity'] > 0:
            trade['price'] = abs(trade['net_proceeds'] / trade['quantity'])
            
            # Adjust for options
            if trade['is_option'] and trade['price'] < 100:
                trade['price'] = trade['price'] * 100
        
        # Log successful option detection with complete details
        if trade['is_option']:
            opt_details = f"Detected option: {trade['symbol']} {trade['option_type'] or 'UNKNOWN'}"
            if trade['strike_price']:
                opt_details += f" @ ${trade['strike_price']}"
            if trade['expiry_date']:
                exp_date = trade['expiry_date']
                if isinstance(exp_date, datetime):
                    opt_details += f" expiring {exp_date.strftime('%Y-%m-%d')}"
                else:
                    opt_details += f" expiring {exp_date}"
            print(opt_details)
            
            # Debug the saved option attributes
            print(f"Option data being saved: type={trade['option_type']}, strike={trade['strike_price']}, expiry={trade['expiry_date']}, dte={trade.get('dte')}")
        
        return trade

    def parse_csv_row(self, row, row_number=0):
        """Parse a CSV row into a trade object"""
        # Process through our primary row parser
        processed_trade = self.process_row(row)
        
        # Skip rows that don't represent trades
        if not processed_trade:
            return None
        
        return processed_trade