#!/usr/bin/env python3
from datetime import datetime
import re
import sys
from typing import Dict, Any, Optional
import time

from .base_broker import BaseBroker
from .symbol_enhancer import (
    lookup_stock_symbol, 
    needs_enhancement, 
    lookup_spac_merger, 
    search_spac_info_with_llm,
    identify_potential_spac
)


class FidelityBroker(BaseBroker):
    """Fidelity specific CSV processing logic with SQLModel field alignment"""
    
    @property
    def use_symbol_enhancement(self) -> bool:
        """Whether this broker should use symbol enhancement"""
        return True
    
    @property
    def column_mappings(self) -> Dict[str, str]:
        """Map Fidelity columns to standardized fields that match SQLModel model"""
        return {
            # Primary column names
            'Run Date': 'date',              # Maps to date
            'Symbol': 'symbol',              # Maps to symbol
            'Description': 'description',    # Maps to description
            'Action': 'side',                # Maps to side
            'Quantity': 'quantity',          # Maps to quantity
            'Price ($)': 'price',            # Maps to price
            'Commission ($)': 'commission',  # Maps to commission
            'Fees ($)': 'fees',              # Additional fees
            'Amount ($)': 'net_proceeds',    # Maps to net_proceeds
            
            # Alternative column names
            'Date': 'date',                  # Alternative date column
            'Trade Date': 'date',            # Alternative date column
            'Activity Date': 'date',         # Alternative date column
            'Type': 'side',                  # Alternative side column
            'Transaction Type': 'side',      # Alternative side column
            'Trans Code': 'side',            # Alternative side column
            'Price': 'price',                # Alternative price column
            'Trade Price': 'price',          # Alternative price column
            'Commission': 'commission',      # Alternative commission column
            'Fees': 'fees',                  # Alternative fees column
            'Amount': 'net_proceeds'         # Alternative net_proceeds column
        }
    
    def process_symbol(self, symbol: str, description: Optional[str] = None) -> str:
        """Process and enhance the symbol if needed"""
        if not symbol:
            return symbol
            
        # Remove any spaces in the symbol - use aggressive cleaning
        symbol = ''.join(symbol.strip().split())
            
        # Store the original symbol for comparison
        original_symbol = symbol.upper()
        
        # Skip enhancement if disabled
        if not self.use_symbol_enhancement:
            return original_symbol
            
        # Special case for Virgin Galactic (always use SPCE)
        if description and "VIRGIN GALACTIC" in description.upper():
            return "SPCE"
        
        # Use symbol_enhancer to determine if this symbol needs enhancement
        if needs_enhancement(original_symbol):
            # Use lookup_stock_symbol from symbol_enhancer
            enhanced_symbol = lookup_stock_symbol(original_symbol, description)
            return enhanced_symbol
                
        return original_symbol
    
    def extract_option_details(self, description: str, symbol: Optional[str] = None, 
                              extra_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Extract option details from Fidelity description format"""
        if not description:
            return {
                'isOption': False,
                'optionType': None,
                'strikePrice': None,
                'expiryDate': None
            }
            
        # Pattern for formats like: "GOOG 6/9/2023 Call $123.00"
        call_put_pattern = r'([A-Z]+)\s+(\d+/\d+/\d+)\s+(Call|Put)\s+\$?(\d+(?:\.\d+)?)'
        match = re.search(call_put_pattern, description, re.IGNORECASE)
        
        details = {
            'isOption': False,
            'optionType': None,
            'strikePrice': None,
            'expiryDate': None
        }
        
        if match:
            symbol_from_desc = match.group(1)
            expiry_str = match.group(2)
            option_type = match.group(3)
            strike_price = match.group(4)
            
            try:
                expiry_date = datetime.strptime(expiry_str, '%m/%d/%Y')
                
                details['isOption'] = True
                details['optionType'] = 'CALL' if option_type.upper() == 'CALL' else 'PUT'
                details['strikePrice'] = float(strike_price)
                details['expiryDate'] = expiry_date
            except ValueError as e:
                print(f"Warning: Could not parse option date: {e}", file=sys.stderr)
        
        # Check keywords for options
        elif any(keyword in description.upper() for keyword in ['CALL', 'PUT', 'OPTION']):
            details['isOption'] = True
            
            # Determine option type
            if 'CALL' in description.upper():
                details['optionType'] = 'CALL'
            elif 'PUT' in description.upper():
                details['optionType'] = 'PUT'
            
            # Try to extract strike price
            price_match = re.search(r'\$?(\d+(?:\.\d+)?)', description)
            if price_match:
                details['strikePrice'] = float(price_match.group(1))
                
            # Try to extract expiry date
            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', description)
            if date_match:
                try:
                    details['expiryDate'] = datetime.strptime(date_match.group(1), '%m/%d/%Y')
                except ValueError:
                    pass
        
        return details
    
    def extract_base_symbol(self, description: str) -> str:
        """Extract a potential stock symbol from a description"""
        if not description:
            return ""
            
        # First, try to extract a clear symbol from the text
        # Look for patterns like "SYMBOL - Company Name" or "(SYMBOL)"
        symbol_patterns = [
            r'([A-Z]{1,5})\s+-\s+',               # AAPL - Apple Inc.
            r'\(([A-Z]{1,5})\)',                   # (AAPL)
            r'^([A-Z]{1,5})\s',                    # AAPL at start of description
            r'CUSIP\s+\d+\s+([A-Z]{1,5})',         # CUSIP 123456789 AAPL
            r'CUSIP[:\s]*(\w+)',                   # Extract CUSIP itself if no clear symbol
        ]
        
        for pattern in symbol_patterns:
            match = re.search(pattern, description)
            if match:
                return match.group(1).strip().upper()
        
        # Special case for Virgin Galactic (SPCE)
        if "VIRGIN GALACTIC" in description.upper():
            return "SPCE"
            
        # Special case for other known companies with unique identifiers
        known_companies = {
            "APPLE": "AAPL",
            "MICROSOFT": "MSFT",
            "AMAZON": "AMZN", 
            "GOOGLE": "GOOGL",
            "FACEBOOK": "META",
            "NETFLIX": "NFLX"
        }
        
        for company, symbol in known_companies.items():
            if company in description.upper():
                return symbol
        
        # Fallback: Try to find any word that looks like a symbol
        # This is a last resort and may not be accurate
        words = description.split()
        for word in words:
            # Clean the word of any non-alphanumeric characters
            cleaned_word = re.sub(r'[^A-Za-z0-9]', '', word).upper()
            # Check if it looks like a stock symbol (1-5 capital letters)
            if re.match(r'^[A-Z]{1,5}$', cleaned_word):
                return cleaned_word
                
        return ""
    
    def infer_trade_side_from_description(self, description: str, amount: float = 0.0, quantity: float = 0.0) -> Optional[str]:
        """Use text analysis to determine if a description indicates a buy or sell"""
        if not description:
            return None
        
        # Step 1: Look for clear buy/sell indicators "you bought" or "you sold" in the description
        desc_upper = description.upper()
        
        # Priority 1: Explicit "YOU BOUGHT" or "YOU SOLD" phrases
        if "YOU BOUGHT" in desc_upper:
            print(f"Explicit BUY found in description: '{description}'")
            return 'BUY'
        elif "YOU SOLD" in desc_upper:
            print(f"Explicit SELL found in description: '{description}'")
            return 'SELL'
        
        # Priority 2: Other buy/sell phrase indicators
        buy_phrases = ['PURCHASE', 'PURCHASES', 'PURCHASED', 'REINVEST', 'SHARES ADDED', 
                    'SHARES ACQUIRED', 'BUY', 'BOUGHT', 'BUYING', 'DEPOSIT']
        sell_phrases = ['SALE', 'SALES', 'SOLD', 'SELL', 'SELLING', 'SHARES REMOVED', 
                      'SHARES REDEEMED', 'REDEMPTION', 'WITHDRAWAL']
        
        # Check for buy phrases
        for phrase in buy_phrases:
            if phrase.upper() in desc_upper:
                print(f"Inferred BUY from description phrase: '{phrase}' in '{description}'")
                return 'BUY'
                
        # Check for sell phrases
        for phrase in sell_phrases:
            if phrase.upper() in desc_upper:
                print(f"Inferred SELL from description phrase: '{phrase}' in '{description}'")
                return 'SELL'
        
        # Priority 3: Transaction quantity (NEW - prioritized over amount)
        if quantity < 0:
            print(f"Inferred SELL from negative quantity: {quantity}")
            return 'SELL'
        elif quantity > 0:
            print(f"Inferred BUY from positive quantity: {quantity}")
            return 'BUY'
        
        # Priority 4: Transaction amount (fallback if quantity doesn't provide direction)
        if amount < 0:
            print(f"Inferred SELL from negative amount: {amount}")
            return 'SELL'
        elif amount > 0:
            print(f"Inferred BUY from positive amount: {amount}")
            return 'BUY'
        
        # Priority 5: Advanced pattern matching
        if re.search(r'(ADDED|ADD|DEPOSIT|TRANSFER IN|CONTRIB|CONTRIBUTION)', desc_upper):
            print(f"Inferred BUY from pattern matching: '{description}'")
            return 'BUY'
        elif re.search(r'(REMOVED|REMOVE|WITHDRAWAL|TRANSFER OUT|DISTRIB|DISTRIBUTION)', desc_upper):
            print(f"Inferred SELL from pattern matching: '{description}'")
            return 'SELL'
            
        return None
    
    def extract_date_from_description(self, description: str) -> Optional[datetime]:
        """Extract a date from a description string using pattern matching"""
        if not description:
            return None
            
        # Common date patterns
        date_patterns = [
            # MM/DD/YYYY
            r'(\d{1,2}/\d{1,2}/\d{4})',
            # Month DD, YYYY
            r'(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},?\s+\d{4}',
            # YYYY-MM-DD
            r'(\d{4}-\d{1,2}-\d{1,2})',
            # DD-MM-YYYY or DD.MM.YYYY
            r'(\d{1,2}[-./]\d{1,2}[-./]\d{4})'
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, description)
            if match:
                date_str = match.group(0)
                return self.parse_complex_date(date_str)
                
        return None
        
    def parse_complex_date(self, date_string: str) -> Optional[datetime]:
        """Advanced date parser that handles multiple formats"""
        if not date_string:
            return None
            
        # Standard formats to try
        formats = [
            '%m/%d/%Y',      # 03/15/2023
            '%Y-%m-%d',      # 2023-03-15
            '%d-%m-%Y',      # 15-03-2023
            '%d.%m.%Y',      # 15.03.2023
            '%B %d, %Y',     # March 15, 2023
            '%b %d, %Y',     # Mar 15, 2023
            '%B %d %Y',      # March 15 2023
            '%b %d %Y',      # Mar 15 2023
            '%d %B %Y',      # 15 March 2023
            '%d %b %Y',      # 15 Mar 2023
            '%Y/%m/%d'       # 2023/03/15
        ]
        
        # Try each format
        for fmt in formats:
            try:
                return datetime.strptime(date_string.strip(), fmt)
            except ValueError:
                continue
                
        # If none of the formats worked, try to extract components
        # Look for patterns like "March 15, 2023" or "15th of March, 2023"
        month_match = re.search(r'(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)', date_string, re.IGNORECASE)
        day_match = re.search(r'\b(\d{1,2})(?:st|nd|rd|th)?\b', date_string)
        year_match = re.search(r'\b(20\d{2})\b', date_string)  # Assumes years in 2000s
        
        if month_match and day_match and year_match:
            month_names = {
                'jan': 1, 'january': 1,
                'feb': 2, 'february': 2,
                'mar': 3, 'march': 3,
                'apr': 4, 'april': 4,
                'may': 5, 'may': 5,
                'jun': 6, 'june': 6,
                'jul': 7, 'july': 7,
                'aug': 8, 'august': 8,
                'sep': 9, 'september': 9,
                'oct': 10, 'october': 10,
                'nov': 11, 'november': 11,
                'dec': 12, 'december': 12
            }
            
            month = month_names.get(month_match.group(0).lower(), 1)
            day = int(day_match.group(1))
            year = int(year_match.group(1))
            
            try:
                return datetime(year, month, day)
            except ValueError:
                pass
                
        # If still not parsed, try to handle numeric dates with different separators
        numeric_match = re.match(r'(\d{1,4})[-./](\d{1,2})[-./](\d{1,4})', date_string.strip())
        if numeric_match:
            a, b, c = map(int, numeric_match.groups())
            
            # Try to determine which is year, month, day
            candidates = []
            
            # If one number is > 31, it's likely the year
            if a > 31 and 1 <= b <= 12 and 1 <= c <= 31:
                # Format: YYYY-MM-DD
                candidates.append(datetime(a, b, c))
            elif a > 31 and 1 <= c <= 12 and 1 <= b <= 31:
                # Format: YYYY-DD-MM
                candidates.append(datetime(a, c, b))
            elif c > 31 and 1 <= a <= 12 and 1 <= b <= 31:
                # Format: MM-DD-YYYY
                candidates.append(datetime(c, a, b))
            elif c > 31 and 1 <= b <= 12 and 1 <= a <= 31:
                # Format: DD-MM-YYYY
                candidates.append(datetime(c, b, a))
            elif b > 31 and 1 <= a <= 12 and 1 <= c <= 31:
                # Format: MM-YYYY-DD
                candidates.append(datetime(b, a, c))
            elif b > 31 and 1 <= c <= 12 and 1 <= a <= 31:
                # Format: DD-YYYY-MM
                candidates.append(datetime(b, c, a))
            
            # Filter out future dates (unlikely for trading)
            now = datetime.now()
            valid_candidates = [dt for dt in candidates if dt <= now]
            
            if valid_candidates:
                return valid_candidates[0]
                
        # Fallback for US dates (MM/DD/YYYY)
        us_date = re.match(r'(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})', date_string.strip())
        if us_date:
            month, day, year = map(int, us_date.groups())
            try:
                if 1 <= month <= 12 and 1 <= day <= 31:
                    return datetime(year, month, day)
            except ValueError:
                pass
        
        # If all fails, return None
        print(f"Could not parse date: '{date_string}'")
        return None
    
    def should_skip_transaction(self, row: Dict[str, str], action: Optional[str] = None, 
                              description: Optional[str] = None) -> bool:
        """Determine if this transaction should be skipped (not a trade)"""
        # Special case for Virgin Galactic - never skip these
        if description and "VIRGIN GALACTIC" in description.upper():
            return False
            
        # Skip certain definite non-trade actions by their action type
        if action and action.upper() in [
            'DIVIDEND', 'INTEREST', 'JOURNAL', 'ADJ', 
            'REINVESTMENT', 'DIV', 'INT', 'FEE', 'REINVEST', 
            'ELECTRONIC FUNDS TRANSFER', 'WIRE', 'ATM', 'CHECK', 
            'ADJUSTMENT', 'DISTRIBUTION'
        ]:
            print(f"Skipping non-trade action type: {action}")
            return True
            
        # Check description for non-trade indicators
        if description:
            description_upper = description.upper()
            non_trade_phrases = [
                'DIVIDEND', 'INTEREST', 'JOURNAL', 'ADJUSTMENT',
                'SERVICE FEE', 'ACCOUNT FEE', 'MARGIN INTEREST', 'WIRE TRANSFER',
                'ELECTRONIC FUNDS TRANSFER', 'ATM', 'CHECK'
            ]
            
            # Skip if description contains any of the non-trade phrases
            # UNLESS it also contains "YOU BOUGHT" or "YOU SOLD" which indicates a trade
            has_trade_indicator = (
                "YOU BOUGHT" in description_upper or 
                "YOU SOLD" in description_upper or
                "VIRGIN GALACTIC" in description_upper  # Always keep Virgin Galactic
            )
            
            if not has_trade_indicator:
                for phrase in non_trade_phrases:
                    if phrase in description_upper:
                        print(f"Skipping based on description containing '{phrase}': {description}")
                        return True
            
        # No reason to skip
        return False
    
    def process_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Process a single row of Fidelity data into SQLModel-compatible format"""
        # Identify the action field
        action = None
        for col in ['Action', 'Type', 'Transaction Type', 'Trans Code']:
            if col in row and row[col]:
                action = row[col]
                break
        
        # Create object with SQLModel Trade model field names
        trade = {
            'timestamp': None,               # Will be generated from date
            'date': None,                    # Will be set from Run Date
            'time': None,                    # Not directly available
            'symbol': None,                  # Will be set from Symbol
            'price': 0.0,                    # Will be set from Price ($)
            'quantity': 0.0,                 # Will be set from Quantity
            'side': None,                    # Will be derived from Action
            'status': 'COMPLETED',           # Default status
            'commission': 0.0,               # Will be set from Commission ($)
            'net_proceeds': 0.0,             # Will be set from Amount ($)
            'is_option': False,              # Will be determined from Description
            'option_type': None,             # Will be extracted from Description
            'strike_price': None,            # Will be extracted from Description
            'expiry_date': None,             # Will be extracted from Description
            'description': None,             # Will be set from Description
            'broker_type': 'fidelity'        # Hardcoded for Fidelity
        }
        
        # Map Fidelity fields to SQLModel fields using our mapping
        for fidelity_col, sqlmodel_field in self.column_mappings.items():
            if fidelity_col in row and row[fidelity_col]:
                trade[sqlmodel_field] = row[fidelity_col]
        
        # Get description field for early check if this is a trade we should process
        has_description = 'description' in trade and trade['description']
        description = trade.get('description', '')
        
        # Check for Virgin Galactic in description - always process these
        is_virgin_galactic = has_description and "VIRGIN GALACTIC" in description.upper()
        
        # Step 0: Skip non-trade transactions early
        if not is_virgin_galactic and self.should_skip_transaction(row, action, description):
            return None
        
        # Process quantity early for use in direction determination
        has_quantity = False
        raw_quantity = 0.0
        
        if 'quantity' in trade and trade['quantity']:
            qty_str = str(trade['quantity']).replace(',', '')
            try:
                qty = float(qty_str)
                raw_quantity = qty  # Store the original quantity for direction determination
                if qty != 0:  # Non-zero quantity is important
                    trade['quantity'] = abs(qty)  # Use absolute value for quantity
                    has_quantity = True
            except (ValueError, TypeError):
                pass  # Invalid quantity
                
        # Process amount/net proceeds early for use in direction determination
        raw_amount = 0.0
        
        if 'net_proceeds' in trade and trade['net_proceeds']:
            amt_str = str(trade['net_proceeds']).replace('$', '').replace(',', '')
            try:
                amount = float(amt_str)
                raw_amount = amount  # Store for direction determination
                trade['net_proceeds'] = amount
            except (ValueError, TypeError):
                pass  # Invalid amount
        
        # Step 1: Try to determine side from description (highest priority)
        side_determined = False
        if has_description:
            # Pass both quantity and amount to the inference function, with quantity being prioritized
            inferred_side = self.infer_trade_side_from_description(
                description, 
                raw_amount,
                raw_quantity
            )
            if inferred_side:
                trade['side'] = inferred_side
                side_determined = True
                print(f"Side determined from description: {inferred_side}")
        
        # If side not determined yet, try to get it from action field
        if not side_determined and 'side' in trade and trade['side']:
            # Process side (direction) - ensure it's always BUY or SELL
            buy_terms = ['BUY', 'BTO', 'BTC', 'BOUGHT', 'PURCHASED', 'YOU BOUGHT']
            sell_terms = ['SELL', 'STO', 'STC', 'SOLD', 'YOU SOLD']
            
            side = trade['side'].upper()
            if any(buy_term in side for buy_term in buy_terms):
                trade['side'] = 'BUY'
                side_determined = True
                print(f"Side determined from action: BUY")
            elif any(sell_term in side for sell_term in sell_terms):
                trade['side'] = 'SELL'
                side_determined = True
                print(f"Side determined from action: SELL")
                
        # Check for price - essential for trade entries
        has_price = False
        
        # Process price
        if 'price' in trade and trade['price']:
            price_str = str(trade['price']).replace('$', '').replace(',', '')
            try:
                price = float(price_str)
                if price > 0:  # Valid price is important
                    trade['price'] = price
                    has_price = True
            except (ValueError, TypeError):
                pass  # Invalid price
                
        # Special handling for CASH entries (most common challenge)
        if not side_determined and action and action.upper() in ['CASH', 'SHARES']:
            # If side not determined for CASH/SHARES but description contains
            # "VIRGIN GALACTIC" or other known stocks, attempt to set side
            if has_description:
                desc = trade['description'].upper()
                
                # Use raw quantity first for direction (NEW PRIORITY)
                if raw_quantity != 0:
                    if raw_quantity < 0:
                        trade['side'] = 'SELL'
                        side_determined = True
                        print(f"CASH/SHARES with negative quantity ({raw_quantity}): Setting side to SELL")
                    else:
                        trade['side'] = 'BUY'
                        side_determined = True
                        print(f"CASH/SHARES with positive quantity ({raw_quantity}): Setting side to BUY")
                
                # Fallback to amount if no quantity direction
                elif raw_amount != 0:
                    if raw_amount < 0:
                        trade['side'] = 'SELL'
                        side_determined = True
                        print(f"CASH/SHARES with negative amount ({raw_amount}): Setting side to SELL")
                    else:
                        trade['side'] = 'BUY'
                        side_determined = True
                        print(f"CASH/SHARES with positive amount ({raw_amount}): Setting side to BUY")
                
                if "VIRGIN GALACTIC" in desc:
                    # For Virgin Galactic - if direction still not determined, default to BUY
                    if not side_determined:
                        trade['side'] = 'BUY'
                        side_determined = True
                        print(f"VIRGIN GALACTIC without direction indicators: Defaulting to BUY")
                    
                    # Ensure we have a symbol set
                    if not trade.get('symbol'):
                        trade['symbol'] = 'SPCE'
                        print(f"Set symbol to SPCE for Virgin Galactic transaction")
                        
                    # For Virgin Galactic, force price and quantity if missing
                    if not has_price:
                        # Default to $1 price if we don't have it
                        trade['price'] = 1.0
                        has_price = True
                        print(f"Setting default price for VIRGIN GALACTIC")
                    
                    if not has_quantity:
                        # Default to 1 share if we don't have it
                        trade['quantity'] = 1.0
                        has_quantity = True
                        print(f"Setting default quantity for VIRGIN GALACTIC")
                
                # For other transfer patterns
                elif not side_determined and 'TRANSFER' in desc:
                    if 'IN' in desc or 'TO' in desc:
                        trade['side'] = 'BUY'
                        side_determined = True
                        print(f"CASH/SHARES transfer in: Setting side to BUY")
                    elif 'OUT' in desc or 'FROM' in desc:
                        trade['side'] = 'SELL'
                        side_determined = True
                        print(f"CASH/SHARES transfer out: Setting side to SELL")
            
            # Default CASH/SHARES to BUY if we have quantity and price but couldn't determine side
            if not side_determined and has_quantity and has_price:
                trade['side'] = 'BUY'
                side_determined = True
                print(f"Defaulting CASH/SHARES action to BUY based on presence of quantity and price")
                
        # Special handling for Virgin Galactic - always include these
        if is_virgin_galactic:
            # Determine side if not already done
            if not side_determined:
                # Check quantity first (NEW PRIORITY)
                if raw_quantity != 0:
                    if raw_quantity < 0:
                        trade['side'] = 'SELL'
                    else:
                        trade['side'] = 'BUY'
                # Then check amount
                elif raw_amount != 0:
                    if raw_amount < 0:
                        trade['side'] = 'SELL'
                    else:
                        trade['side'] = 'BUY'
                else:
                    # Default to BUY if no direction indicators
                    trade['side'] = 'BUY'
                
                side_determined = True
                print(f"Forced side for VIRGIN GALACTIC: {trade['side']}")
                
            # Ensure we have a symbol
            if not trade.get('symbol'):
                trade['symbol'] = 'SPCE'
                print(f"Setting symbol to SPCE for Virgin Galactic")
                
            # If we have missing price/quantity, set defaults
            if not has_price:
                trade['price'] = 1.0
                has_price = True
                print(f"Setting default price=1.0 for VIRGIN GALACTIC")
                
            if not has_quantity:
                trade['quantity'] = 1.0
                has_quantity = True
                print(f"Setting default quantity=1.0 for VIRGIN GALACTIC")
        
        # Step 2: Process symbol - with enhancement if it contains digits
        if 'symbol' in trade and trade['symbol']:
            # Aggressively strip all spaces from the symbol
            original_symbol = ''.join(str(trade['symbol']).strip().split()).upper()
            
            # Check if this might be a SPAC based on description
            is_potential_spac = False
            if has_description:
                is_potential_spac = identify_potential_spac(description)
                if is_potential_spac:
                    print(f"Detected potential SPAC from description: {description}")
            
            # First apply SPAC resolution if we have a description and the symbol might be a SPAC
            if has_description:
                resolved_symbol = self.resolve_spac_symbol(original_symbol, description)
                if resolved_symbol != original_symbol:
                    print(f"Resolved SPAC symbol from {original_symbol} to {resolved_symbol}")
                    original_symbol = resolved_symbol
                    trade['symbol'] = resolved_symbol
                    trade['symbol_resolved'] = True
                    trade['is_spac'] = True
            
            # Check if the symbol needs enhancement (contains digits)
            needs_enhancement_flag = needs_enhancement(original_symbol)
            
            if needs_enhancement_flag:
                print(f"Symbol {original_symbol} contains digits, enhancing...")
                
            enhanced_symbol = self.process_symbol(original_symbol, trade.get('description'))
            
            # Track if symbol was enhanced
            if enhanced_symbol != original_symbol:
                trade['original_symbol'] = original_symbol
                trade['symbol'] = enhanced_symbol
                trade['symbol_enhanced'] = True
                print(f"Enhanced symbol from {original_symbol} to {enhanced_symbol}")
                
                # For Virgin Galactic special case
                if "VIRGIN GALACTIC" in str(trade.get('description', '')).upper() and enhanced_symbol != "SPCE":
                    trade['symbol'] = 'SPCE'
                    trade['is_spac'] = True
                    print(f"Overriding enhanced symbol to SPCE for Virgin Galactic")
            
            # If we identified it as a potential SPAC but couldn't resolve, mark it for review
            if is_potential_spac and not trade.get('symbol_resolved'):
                trade['potential_spac'] = True
                print(f"Marked {trade['symbol']} as potential unresolved SPAC")
        
        # If we don't have a symbol, try to extract it from description
        elif trade.get('description'):
            # Try to extract symbol from description
            raw_symbol = self.extract_base_symbol(trade['description'])
            original_symbol = raw_symbol
            
            if raw_symbol:
                # First apply SPAC resolution
                resolved_symbol = self.resolve_spac_symbol(original_symbol, description)
                if resolved_symbol != original_symbol:
                    print(f"Resolved SPAC symbol from {original_symbol} to {resolved_symbol}")
                    original_symbol = resolved_symbol
                    trade['symbol_resolved'] = True
                
                # Then apply normal symbol enhancement
                enhanced_symbol = self.process_symbol(original_symbol, trade['description'])
                
                trade['symbol'] = enhanced_symbol
                
                # Track if symbol was enhanced
                if enhanced_symbol != original_symbol:
                    trade['original_symbol'] = original_symbol
                    trade['symbol_enhanced'] = True
                    print(f"Extracted and enhanced symbol from {original_symbol} to {enhanced_symbol}")
            else:
                # No symbol could be extracted - this might not be a valid trade
                # But for Virgin Galactic, we always want to process
                if is_virgin_galactic:
                    trade['symbol'] = 'SPCE'
                    print(f"Setting symbol to SPCE for Virgin Galactic (no symbol in description)")
                else:
                    print(f"Could not extract symbol from description: {trade.get('description')}")
                    if not has_quantity or not has_price:
                        print(f"Skipping transaction without symbol, quantity, or price")
                        return None
                    
                    trade['symbol'] = 'UNKNOWN'
        
        # Step 3: If no symbol by this point and not a clear trade, skip it
        if not trade.get('symbol') or trade.get('symbol') == 'UNKNOWN':
            # But for Virgin Galactic, we always want to process
            if is_virgin_galactic:
                trade['symbol'] = 'SPCE'
                print(f"Setting symbol to SPCE for Virgin Galactic (no symbol detected)")
            elif not side_determined or not has_quantity or not has_price:
                print(f"Skipping transaction without clear symbol and trade indicators")
                return None
        
        # Check for the presence of SPAC indicators in description if not already resolved
        if has_description and 'symbol' in trade and not trade.get('symbol_resolved'):
            # These are keywords that might indicate a SPAC transaction
            spac_keywords = ["SPAC", "ACQUISITION", "HOLDINGS", "CAPITAL CORP", "BLANK CHECK"]
            
            if any(keyword in description.upper() for keyword in spac_keywords):
                # This might be a SPAC that needs special handling
                # In a production environment, we would call a web search API here
                print(f"Potential SPAC detected: {trade.get('symbol')} - {description}")
                
                # Try to resolve using our static mapping
                resolved_symbol = self.resolve_spac_symbol(trade.get('symbol', ''), description)
                if resolved_symbol != trade.get('symbol'):
                    print(f"Resolved SPAC symbol from {trade.get('symbol')} to {resolved_symbol}")
                    trade['original_symbol'] = trade.get('symbol')
                    trade['symbol'] = resolved_symbol
                    trade['symbol_resolved'] = True
        
        # At this point, if we still don't have a side but have both quantity and price,
        # it's likely a valid trade missing directional info
        if not side_determined and has_quantity and has_price:
            # Final fallback - default to BUY with a meaningful log
            trade['side'] = 'BUY'
            side_determined = True
            print(f"Final fallback: Setting side to BUY with quantity={trade.get('quantity')}, price={trade.get('price')}")
                
        # If we still couldn't determine the side and essential trade data is missing, skip this row
        # UNLESS it's Virgin Galactic
        if not is_virgin_galactic and (not side_determined or not has_quantity or not has_price):
            missing = []
            if not side_determined:
                missing.append("side")
            if not has_quantity:
                missing.append("quantity")
            if not has_price:
                missing.append("price")
            print(f"Skipping row - Missing required fields: {missing}")
            return None
                
        # ENHANCED DATE PARSING LOGIC
        date_parsed = False
        date_obj = None
        
        # First priority: Check for 'Run Date' in the original row
        if 'Run Date' in row and row['Run Date']:
            date_str = row['Run Date']
            date_obj = self.parse_complex_date(date_str)
            if date_obj:
                date_parsed = True
                print(f"Date parsed from Run Date: {date_str} -> {date_obj.strftime('%Y-%m-%d')}")
                
        # Second priority: Look for mapped date fields in trade dictionary
        if not date_parsed and 'date' in trade and trade['date']:
            date_str = trade['date']
            date_obj = self.parse_complex_date(date_str)
            if date_obj:
                date_parsed = True
                print(f"Date parsed from mapped date field: {date_str} -> {date_obj.strftime('%Y-%m-%d')}")
                
        # Third priority: Try to extract date from description
        if not date_parsed and has_description:
            date_obj = self.extract_date_from_description(trade['description'])
            if date_obj:
                date_parsed = True
                print(f"Date extracted from description -> {date_obj.strftime('%Y-%m-%d')}")
                
        # Fourth priority: Look for date in any field as a last resort
        if not date_parsed:
            for col, val in row.items():
                # Skip fields we've already checked or that are unlikely to contain dates
                if col in ['Run Date', 'Symbol', 'Quantity', 'Price ($)', 'Amount ($)'] or not val:
                    continue
                    
                date_obj = self.parse_complex_date(str(val))
                if date_obj:
                    date_parsed = True
                    print(f"Date found in field '{col}': {val} -> {date_obj.strftime('%Y-%m-%d')}")
                    break
        
        # If we still don't have a date, use current date
        if not date_parsed or not date_obj:
            date_obj = datetime.now()
            print(f"No valid date found, using current date: {date_obj.strftime('%Y-%m-%d')}")
        
        # Set the timestamp and formatted date
        trade['timestamp'] = date_obj
        trade['date'] = date_obj.strftime('%Y-%m-%d')
        trade['time'] = date_obj.strftime('%H:%M:%S')
        
        # Process numeric values - handle missing fields gracefully
        for field in ['price', 'quantity', 'commission', 'net_proceeds']:
            if field in trade and trade[field]:
                trade[field] = self.clean_numeric(trade[field])
            else:
                trade[field] = 0.0
        
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

    def resolve_spac_symbol(self, symbol: str, description: Optional[str] = None) -> str:
        """Use the symbol_enhancer to resolve SPAC symbols"""
        if not symbol:
            return symbol
            
        # Use our new lookup_spac_merger function
        original, resolved = lookup_spac_merger(symbol, description)
        
        # Log detailed information about SPACs for analysis
        if original != resolved:
            try:
                # Try to get more detailed info using the LLM search
                spac_info = search_spac_info_with_llm(symbol, description)
                if spac_info and spac_info.get("merger_status") == "completed":
                    print(f"SPAC detailed info: {symbol} → {spac_info.get('current_symbol')} "
                          f"(merged with {spac_info.get('target_company')} on {spac_info.get('merger_date')})")
            except Exception as e:
                print(f"Error getting detailed SPAC info: {e}")
                
        return resolved 