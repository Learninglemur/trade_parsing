#!/usr/bin/env python3
import csv
import re
import sys
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Optional, Any

class BaseBroker(ABC):
    """Base class for broker-specific CSV processing"""
    
    @property
    @abstractmethod
    def column_mappings(self):
        """Map broker-specific columns to standardized fields"""
        pass
    
    @property
    def use_symbol_enhancement(self):
        """Whether this broker should use symbol enhancement"""
        return False
    
    def extract_base_symbol(self, symbol_text):
        """Extract the base symbol from potentially complex option symbols or descriptions."""
        if not symbol_text:
            return "UNKNOWN"
            
        # If it's just a plain symbol with no spaces or special chars
        if re.match(r'^[A-Z]+$', symbol_text):
            return symbol_text
        
        # Schwab option format: "OEX 12/19/2009 495.00 C"
        schwab_option_match = re.search(r'^([A-Z]+)\s+\d+/\d+/\d+', symbol_text)
        if schwab_option_match:
            return schwab_option_match.group(1)
            
        # Robinhood format: "GOOG 6/9/2023 Call $123.00"
        option_match = re.search(r'^([A-Z]+)\s', symbol_text)
        if option_match:
            return option_match.group(1)
            
        # Interactive Brokers format: "SPX 15MAR24 5140 P"
        ib_match = re.search(r'^([A-Z]+)\s+\d+[A-Z]+\d+', symbol_text)
        if ib_match:
            return ib_match.group(1)
        
        # TD Ameritrade format: "GEVO INC COM PAR (GEVO)"
        td_match = re.search(r'\(([A-Z]+)\)', symbol_text)
        if td_match:
            return td_match.group(1)
        
        # For more complex symbols, just get the alphabetic characters
        alpha_only = ''.join(c for c in symbol_text if c.isalpha())
        return alpha_only or "UNKNOWN"

    def extract_option_details(self, description, symbol=None, extra_data=None):
        """Extract option details from description with broker-specific logic"""
        if not description and not symbol:
            return None
            
        details = {
            'isOption': False,
            'optionType': None,
            'strikePrice': None,
            'expiryDate': None
        }
        
        # Schwab format: "OEX 12/19/2009 495.00 C"
        schwab_match = re.search(r'^([A-Z]+)\s+(\d+/\d+/\d+)\s+(\d+(?:\.\d+)?)\s+([CP])$', description)
        if schwab_match:
            details['isOption'] = True
            details['optionType'] = 'CALL' if schwab_match.group(4) == 'C' else 'PUT'
            details['strikePrice'] = float(schwab_match.group(3))
            try:
                details['expiryDate'] = datetime.strptime(schwab_match.group(2), '%m/%d/%Y')
            except ValueError:
                pass
        
        # Old Schwab format: "ORBIT INTL C" or "ORBIT INTL P"
        elif description and (description.endswith(' C') or description.endswith(' P')):
            details['isOption'] = True
            details['optionType'] = 'CALL' if description.endswith(' C') else 'PUT'
            
            # For older Schwab format, we might need additional data to get strike/expiry
            if extra_data and 'strikePrice' in extra_data:
                details['strikePrice'] = extra_data['strikePrice']
            if extra_data and 'expiryDate' in extra_data:
                details['expiryDate'] = extra_data['expiryDate']
        
        # Robinhood format: "GOOG 6/9/2023 Call $123.00"
        elif description and ('Call' in description or 'Put' in description):
            details['isOption'] = True
            details['optionType'] = 'CALL' if 'Call' in description else 'PUT'
            
            # Extract strike price
            strike_match = re.search(r'\$(\d+(?:\.\d+)?)', description)
            if strike_match:
                details['strikePrice'] = float(strike_match.group(1))
            
            # Extract expiry date - Robinhood format is M/D/YYYY
            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', description)
            if date_match:
                try:
                    details['expiryDate'] = datetime.strptime(date_match.group(1), '%m/%d/%Y')
                except ValueError:
                    pass
                    
        # Interactive Brokers format: "SPX 15MAR24 5140 P"
        elif symbol and re.search(r'[CP]$', symbol):
            details['isOption'] = True
            details['optionType'] = 'CALL' if symbol.endswith('C') else 'PUT'
            
            # Extract strike price and expiry from IB format
            ib_match = re.search(r'([A-Z]+)\s+(\d+[A-Z]+\d+)\s+(\d+)\s+[CP]', description)
            if ib_match:
                # Extract strike price
                details['strikePrice'] = float(ib_match.group(3))
                
                # Extract expiry date - IB format like "15MAR24"
                date_str = ib_match.group(2)
                try:
                    details['expiryDate'] = datetime.strptime(date_str, '%d%b%y')
                except ValueError:
                    try:
                        # Try alternative format
                        day = date_str[:2]
                        month = date_str[2:5]
                        year = "20" + date_str[5:7]
                        details['expiryDate'] = datetime.strptime(f"{day} {month} {year}", '%d %b %Y')
                    except ValueError:
                        pass
        
        # TD Ameritrade option format might be in extra data
        elif extra_data and 'optionType' in extra_data:
            details['isOption'] = True
            details['optionType'] = extra_data['optionType']
            details['strikePrice'] = extra_data.get('strikePrice')
            details['expiryDate'] = extra_data.get('expiryDate')
        
        return details if details['isOption'] else None

    def determine_direction(self, action):
        """Determine standardized direction (BUY/SELL) from broker-specific action."""
        if not action:
            return None
            
        buy_indicators = ['BUY', 'YOU BOUGHT', 'PURCHASE', 'BOUGHT', 'Buy', 'BTO', 'BTC', 'Buy to Open', 'Buy to Close']
        sell_indicators = ['SELL', 'YOU SOLD', 'SALE', 'SOLD', 'Sell', 'STO', 'STC', 'Sell to Open', 'Sell to Close']
        
        action_upper = action.upper()
        
        for indicator in buy_indicators:
            if indicator.upper() in action_upper:
                return 'BUY'
        
        for indicator in sell_indicators:
            if indicator.upper() in action_upper:
                return 'SELL'
        
        return None  # Return None if direction cannot be determined

    def parse_date(self, date_str):
        """Parse date string to standard ISO format"""
        if not date_str:
            return None
            
        date_formats = [
            '%m/%d/%Y',           # 01/13/2022
            '%Y-%m-%d',           # 2022-01-13
            '%m-%d-%Y',           # 01-13-2022
            '%d/%m/%Y',           # 13/01/2022
            '%Y%m%d',             # 20220113
            '%m/%d/%y',           # 01/13/22
            '%d-%m-%y',           # 13-01-22
            '%b %d, %Y',          # Jan 13, 2022
            '%d %b %Y',           # 13 Jan 2022
            '%m/%d/%Y %H:%M:%S',  # 01/13/2022 14:30:00
            '%Y-%m-%d %H:%M',     # 2024-03-14 10:32
        ]
        
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            except ValueError:
                continue
        
        return None

    def clean_numeric(self, value):
        """Convert string numeric values to float, handling currency symbols and commas"""
        if value is None:
            return 0.0
            
        if isinstance(value, (int, float)):
            return float(value)
            
        # Remove currency symbols, commas, and whitespace
        clean_value = re.sub(r'[$,\s]', '', str(value))
        
        # Handle parentheses for negative numbers
        if clean_value.startswith('(') and clean_value.endswith(')'):
            clean_value = '-' + clean_value[1:-1]
            
        # Handle empty string
        if not clean_value:
            return 0.0
            
        try:
            return float(clean_value)
        except ValueError:
            print(f"Warning: Could not convert '{value}' to float", file=sys.stderr)
            return 0.0
    
    @abstractmethod
    def process_row(self, row):
        """Process a single row of broker data into Prisma-compatible format"""
        pass
    
    def process_csv(self, csv_file):
        """Process the entire CSV file and return array of standardized trade objects"""
        trades = []
        
        try:
            with open(csv_file, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    trade = self.process_row(row)
                    if trade:  # Skip rows that don't represent trades
                        trades.append(trade)
        except Exception as e:
            print(f"Error processing CSV: {str(e)}", file=sys.stderr)
            raise
            
        return trades

    def process_symbol(self, symbol, description=None):
        """Process and enhance the symbol if needed - overridden by broker classes"""
        return symbol

    def parse_csv_row(self, row: Dict[str, str], row_number: int = 0) -> Optional[Dict[str, Any]]:
        """
        Process a single CSV row into a standardized trade dictionary
        This method should be used by the Flask app instead of direct row processing
        
        Args:
            row: A dictionary representing a CSV row
            row_number: The row number for logging purposes
            
        Returns:
            A standardized trade dictionary or None if row should be skipped
        """
        try:
            # Get the processed row using the broker-specific implementation
            processed = self.process_row(row)
            if not processed:
                return None
                
            # Perform any additional common processing here
            return processed
        except Exception as e:
            print(f"Error processing row {row_number}: {str(e)}", file=sys.stderr)
            return None

def get_broker_parser(broker_type):
    """Factory function to get the appropriate broker parser"""
    from .fidelity import FidelityBroker
    from .robinhood import RobinhoodBroker
    from .interactive_brokers import InteractiveBrokersBroker
    from .charles_schwab import CharlesSchwabBroker
    from .tastytrade import TastyTradeBroker
    from .tradingview import TradingViewBroker
    from .webull import WebullBroker
    
    broker_map = {
        'fidelity': FidelityBroker,
        'robinhood': RobinhoodBroker,
        'interactive-brokers': InteractiveBrokersBroker,
        'charles-schwab': CharlesSchwabBroker,
        'tastytrade': TastyTradeBroker,
        'tradingview': TradingViewBroker,
        'webull': WebullBroker
    }
    
    # Handle aliases
    normalized_broker = broker_type.lower().replace(' ', '-')
    if normalized_broker == 'td':
        normalized_broker = 'fidelity'
    elif normalized_broker == 'schwab':
        normalized_broker = 'charles-schwab'
    elif normalized_broker == 'ib':
        normalized_broker = 'interactive-brokers'
    elif normalized_broker == 'td-ameritrade':
        normalized_broker = 'fidelity'
    elif normalized_broker == 'tasty-trade':
        normalized_broker = 'tastytrade'
    elif normalized_broker == 'trading-view':
        normalized_broker = 'tradingview'
    
    if normalized_broker not in broker_map:
        raise ValueError(f"Unsupported broker type: {broker_type}")
        
    return broker_map[normalized_broker]()