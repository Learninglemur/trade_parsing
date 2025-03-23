import os
import re
import csv
import sys
import logging
import hashlib
import traceback
import tempfile
from datetime import datetime
from pathlib import Path
from uuid import uuid4
from typing import Dict, List, Optional, Any, Tuple
from io import StringIO

from flask import Flask, request, jsonify, render_template, send_from_directory
from flask_cors import CORS
from sqlmodel import SQLModel, Session, create_engine, select, Field
from werkzeug.utils import secure_filename

from database import get_session, create_db_and_tables, engine
from models import User, Trade, TradeSide, TradeStatus
from brokers import get_broker_parser

# Only import the broker parsers that actually exist
try:
    from brokers.fidelity import FidelityBroker
except ImportError:
    pass

# Import the symbol enhancer functions - only if they're actually used in the code
try:
    from brokers.symbol_enhancer import lookup_spac_merger, search_spac_info_with_llm
except ImportError:
    # If the import fails, provide stub functions that just return the original symbol
    def lookup_spac_merger(symbol, description=None):
        return (symbol, symbol)
    
    def search_spac_info_with_llm(symbol, description=None):
        return {"original_symbol": symbol, "current_symbol": symbol, "merger_status": "unknown"}

# Create Flask app
app = Flask(__name__, static_folder="./static", template_folder="./templates")
CORS(app)  # Enable CORS for all routes

# Configure uploads directory
UPLOADS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
os.makedirs(UPLOADS_DIR, exist_ok=True)

# Mapping from frontend broker names to internal names
BROKER_NAME_MAPPING = {
    'Fidelity': 'fidelity',
    'InteractiveBrokers': 'interactive-brokers',
    'RobinHood': 'robinhood',
    'Schwab': 'charles-schwab',
    'TastyTrade': 'tastytrade',
    'TradingView': 'tradingview',
    'Webull': 'webull'
}

# Broker-specific column mappings
BROKER_MAPPINGS = {
    'Fidelity': {
        'symbol': 'Symbol',
        'quantity': 'Quantity',
        'price': 'Price ($)',
        'side': 'Action',  # Buy/Sell will be in the Action column
        'date': 'Run Date',
        'description': 'Description',  # Added description field
        'net_proceeds': 'Amount ($)',  # Added net proceeds field
        'side_buy_values': ['BUY', 'BOUGHT', 'PURCHASED', 'YOU BOUGHT'],
        'side_sell_values': ['SELL', 'SOLD', 'YOU SOLD']
    },
    'InteractiveBrokers': {
        'symbol': 'Symbol',
        'quantity': 'Quantity',
        'price': 'Price',
        'side': 'Side',
        'date': 'Date',
        'description': 'Description'  # Added description field
    },
    'RobinHood': {
        'symbol': 'Symbol',
        'quantity': 'Quantity',
        'price': 'Price',
        'side': 'Side',
        'date': 'Date',
        'description': 'Description',  # Added description field
        # Alternative column names for Robinhood format
        'alt_symbol': 'Instrument', 
        'alt_side': 'Trans Code',
        'alt_date': 'Activity Date'
    },
    'Schwab': {
        'symbol': 'Symbol',
        'quantity': 'Quantity',
        'price': 'Price',
        'side': 'Side',
        'date': 'Date',
        'description': 'Description'  # Added description field
    },
    'TastyTrade': {
        'symbol': 'Symbol',
        'quantity': 'Quantity',
        'price': 'Price',
        'side': 'Side',
        'date': 'Date',
        'description': 'Description'  # Added description field
    },
    'TradingView': {
        'symbol': 'Symbol',
        'quantity': 'Quantity',
        'price': 'Price',
        'side': 'Side',
        'date': 'Date',
        'description': 'Description'  # Added description field
    },
    'Webull': {
        'symbol': 'Symbol',
        'quantity': 'Quantity',
        'price': 'Price',
        'side': 'Side',
        'date': 'Date',
        'description': 'Description'  # Added description field
    },
    # Default mapping used if broker-specific mapping is not found
    'Default': {
        'symbol': 'Symbol',
        'quantity': 'Quantity',
        'price': 'Price',
        'side': 'Side',
        'date': 'Date',
        'description': 'Description',  # Added description field
        'side_buy_values': ['BUY', 'Buy', 'buy'],
        'side_sell_values': ['SELL', 'Sell', 'sell']
    }
}

# Create tables
create_db_and_tables()
print("Database tables created.")

# Root endpoint
@app.route('/')
def root():
    return render_template('index.html')

# User endpoints
@app.route('/users', methods=['POST'])
def create_user():
    data = request.json
    name = data.get('name')
    email = data.get('email')
    password = data.get('password', 'default_password')  # Default password for simplicity
    
    with Session(engine) as session:
        user = User(name=name, email=email, password=password)
        session.add(user)
        session.commit()
        session.refresh(user)
        return jsonify({
            "id": user.id, 
            "name": user.name, 
            "email": user.email
        })

@app.route('/users', methods=['GET'])
def read_users():
    with Session(engine) as session:
        users = session.exec(select(User)).all()
        return jsonify([{
            "id": user.id, 
            "name": user.name, 
            "email": user.email
        } for user in users])

@app.route('/trades', methods=['GET'])
def get_trades():
    user_email = request.args.get('email')
    if not user_email:
        return jsonify({"error": "Email parameter is required", "success": False}), 400
    
    with Session(engine) as session:
        # Find the user
        user_query = select(User).where(User.email == user_email)
        user = session.exec(user_query).first()
        
        if not user:
            return jsonify({"error": "User not found", "success": False}), 404
        
        # Get all trades for the user
        trade_query = select(Trade).where(Trade.user_id == user.id)
        trades = session.exec(trade_query).all()
        
        # Format the trades for response, ensuring expiry_date is formatted as YYYY-MM-DD
        formatted_trades = []
        for trade in trades:
            formatted_trade = {
                "id": trade.id,
                "symbol": trade.symbol,
                "quantity": trade.quantity,
                "price": trade.price,
                "side": trade.side,
                "date": trade.date,
                "timestamp": str(trade.timestamp),
                "is_option": trade.is_option,
                "option_type": trade.option_type,
                "strike_price": trade.strike_price,
                "expiry_date": trade.expiry_date.strftime('%Y-%m-%d') if trade.expiry_date else None,
                "dte": trade.dte,
                "description": trade.description,
                "broker_type": trade.broker_type,
                "net_proceeds": trade.net_proceeds
            }
            formatted_trades.append(formatted_trade)
        
        return jsonify({
            "success": True,
            "user_id": user.id,
            "user_email": user.email,
            "trade_count": len(formatted_trades),
            "trades": formatted_trades
        })

# Upload endpoint
@app.route('/upload', methods=['POST'])
def upload_csv():
    try:
        # Validate request
        if 'csvFile' not in request.files:
            print("Error: No file part in the request")
            return jsonify({"error": "No file part in the request", "success": False}), 400
        
        file = request.files['csvFile']
        if not file or file.filename == '':
            print("Error: No file selected")
            return jsonify({"error": "No file selected", "success": False}), 400
        
        broker_type = request.form.get('broker')
        if not broker_type:
            print("Error: No broker specified")
            return jsonify({"error": "No broker specified", "success": False}), 400
        
        user_email = request.form.get('email')
        if not user_email:
            print("Error: No email specified")
            return jsonify({"error": "No email specified", "success": False}), 400
        
        print(f"Processing upload: file={file.filename}, broker={broker_type}, email={user_email}")
        
        # Check if user exists
        with Session(engine) as session:
            try:
                user_query = select(User).where(User.email == user_email)
                user = session.exec(user_query).first()
                
                if not user:
                    print(f"Creating new user with email: {user_email}")
                    # Create new user with default values
                    user = User(
                        email=user_email,
                        name=user_email.split('@')[0],  # Simple name from email
                        password="default_password"  # Default password for simplicity
                    )
                    session.add(user)
                    session.commit()
                    session.refresh(user)
                    print(f"New user created with ID: {user.id}")
                else:
                    print(f"Found existing user with ID: {user.id}")
                
                # Process CSV file
                try:
                    # Check file extension
                    if not file.filename.lower().endswith('.csv'):
                        print(f"Error: File {file.filename} is not a CSV file")
                        return jsonify({"error": "File must be a CSV", "success": False}), 400
                    
                    # Save original file to uploads directory
                    original_filename = file.filename
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    safe_email = user_email.replace('@', '_at_').replace('.', '_dot_')
                    
                    # Create unique filename with timestamp, user email, and broker
                    save_filename = f"{timestamp}_{safe_email}_{broker_type}_{original_filename}"
                    save_path = os.path.join(UPLOADS_DIR, save_filename)
                    
                    # Read the file content first (to avoid seeking issues)
                    content = file.read()
                    if not content:
                        print("Error: Uploaded file is empty")
                        return jsonify({"error": "Uploaded file is empty", "success": False}), 400
                    
                    try:
                        file_content_str = content.decode('utf-8')
                    except UnicodeDecodeError as e:
                        print(f"Error decoding file content: {str(e)}")
                        return jsonify({"error": f"File encoding error: {str(e)}", "success": False}), 400
                    
                    # We'll save only the processed file, not the original upload
                    
                    # Process the CSV content
                    try:
                        # Get the broker-specific parser
                        internal_broker_name = BROKER_NAME_MAPPING.get(broker_type, broker_type.lower())
                        broker_parser = get_broker_parser(internal_broker_name)
                        print(f"Using broker parser: {broker_parser.__class__.__name__}")
                        
                        # Get column mapping from the broker parser
                        mapping = broker_parser.column_mappings
                        print(f"Using column mapping from broker: {broker_type}")
                        
                        csv_reader = csv.DictReader(StringIO(file_content_str))
                        if not csv_reader.fieldnames:
                            print("Error: CSV file has no headers")
                            return jsonify({"error": "CSV file has no headers", "success": False}), 400
                        
                        # Print available columns for debugging
                        print(f"CSV columns: {csv_reader.fieldnames}")
                        
                        # Fix common CSV issues
                        cleaned_fieldnames = [col.strip() if col else "" for col in csv_reader.fieldnames]
                        if cleaned_fieldnames and cleaned_fieldnames[0] and cleaned_fieldnames[0].startswith('\ufeff'):  # BOM character
                            cleaned_fieldnames[0] = cleaned_fieldnames[0].replace('\ufeff', '')
                            print(f"Removed BOM from first column: {cleaned_fieldnames[0]}")
                        
                        # Check if CSV file has the expected structure for the chosen broker
                        # This is a more flexible approach than checking for exact column names
                        if not cleaned_fieldnames or all(not field for field in cleaned_fieldnames):
                            print("Error: CSV file appears to have empty headers")
                            return jsonify({
                                "error": "CSV file has empty headers. Please ensure the first row contains column names.",
                                "success": False,
                                "fieldnames": csv_reader.fieldnames
                            }), 400
                        
                        # Get column mapping for this broker
                        # The broker's column_mappings maps from CSV column name to internal name
                        # So we need to get the reverse mapping for lookups
                        # Find which CSV column maps to 'symbol', 'quantity', etc.
                        symbol_col = None
                        quantity_col = None
                        price_col = None
                        side_col = None
                        date_col = None
                        description_col = None
                        
                        # Find the column names that map to our needed fields
                        for csv_col, field_name in mapping.items():
                            if field_name == 'symbol':
                                symbol_col = csv_col
                            elif field_name == 'quantity':
                                quantity_col = csv_col
                            elif field_name == 'price':
                                price_col = csv_col
                            elif field_name == 'side':
                                side_col = csv_col
                            elif field_name == 'date':
                                date_col = csv_col
                            elif field_name == 'description':
                                description_col = csv_col
                                
                        print(f"Mapped columns - Symbol: {symbol_col}, Quantity: {quantity_col}, Price: {price_col}, Side: {side_col}")
                        
                        # Check if required columns exist or try to infer them
                        found_cols = {
                            'Symbol': False,
                            'Quantity': False,
                            'Price': False,
                            'Side': False
                        }
                        
                        # Try to find required columns in a case-insensitive way
                        fieldnames_lower = [f.lower() if f else "" for f in cleaned_fieldnames]
                        symbol_alternatives = ['symbol', 'ticker', 'stock', 'security', 'instrument']
                        quantity_alternatives = ['quantity', 'qty', 'shares', 'amount', 'volume']
                        price_alternatives = ['price', 'cost', 'price ($)', 'share price']
                        side_alternatives = ['side', 'action', 'type', 'transaction', 'order', 'trans code']
                        
                        # First check if the exact columns exist
                        if symbol_col in cleaned_fieldnames:
                            found_cols['Symbol'] = True
                        if quantity_col in cleaned_fieldnames:
                            found_cols['Quantity'] = True
                        if price_col in cleaned_fieldnames:
                            found_cols['Price'] = True
                        if side_col in cleaned_fieldnames:
                            found_cols['Side'] = True
                            
                        # If not found, try case-insensitive alternatives
                        if not found_cols['Symbol']:
                            for i, col in enumerate(fieldnames_lower):
                                if any(alt in col for alt in symbol_alternatives):
                                    symbol_col = cleaned_fieldnames[i]
                                    found_cols['Symbol'] = True
                                    print(f"Found Symbol column: {symbol_col}")
                                    break
                                    
                        if not found_cols['Quantity']:
                            for i, col in enumerate(fieldnames_lower):
                                if any(alt in col for alt in quantity_alternatives):
                                    quantity_col = cleaned_fieldnames[i]
                                    found_cols['Quantity'] = True
                                    print(f"Found Quantity column: {quantity_col}")
                                    break
                                    
                        if not found_cols['Price']:
                            for i, col in enumerate(fieldnames_lower):
                                if any(alt in col for alt in price_alternatives):
                                    price_col = cleaned_fieldnames[i]
                                    found_cols['Price'] = True
                                    print(f"Found Price column: {price_col}")
                                    break
                                    
                        if not found_cols['Side']:
                            for i, col in enumerate(fieldnames_lower):
                                if any(alt in col for alt in side_alternatives):
                                    side_col = cleaned_fieldnames[i]
                                    found_cols['Side'] = True
                                    print(f"Found Side column: {side_col}")
                                    break
                        
                        # See if we're still missing any columns
                        missing_columns = [k for k, v in found_cols.items() if not v]
                        
                        if missing_columns:
                            print(f"Error: Missing required columns for {broker_type}: {missing_columns}")
                            
                            # Provide more helpful error message
                            col_explanation = {
                                'Symbol': f"Expected: '{symbol_col}' or any column containing stock symbols",
                                'Quantity': f"Expected: '{quantity_col}' or any column with number of shares",
                                'Price': f"Expected: '{price_col}' or any column with price per share",
                                'Side': f"Expected: '{side_col}' or any column indicating buy/sell",
                            }
                            
                            missing_explanations = [col_explanation[col] for col in missing_columns]
                            
                            return jsonify({
                                "error": f"CSV is missing columns needed for {broker_type} format: {missing_columns}\n\nYour CSV contains these columns: {', '.join(cleaned_fieldnames)}\n\nRequired columns are: {', '.join([col_explanation[col] for col in missing_columns])}",
                                "success": False,
                                "fieldnames": cleaned_fieldnames,
                                "missing_columns": missing_columns,
                                "column_explanations": missing_explanations
                            }), 400
                        
                        # Simple trade processing
                        trades = []
                        row_count = 0
                        error_count = 0
                        symbol_enhancement_count = 0
                        potential_spacs = 0  # Initialize potential_spacs counter
                        
                        # Process each row in the CSV using the broker's parser
                        for row in csv_reader:
                            row_count += 1
                            try:
                                # Let the broker parser handle the row
                                processed_trade = broker_parser.parse_csv_row(row, row_count)
                                
                                # Skip rows that return None (not trades or errors)
                                if not processed_trade:
                                    print(f"Skipping row {row_count} - Not a valid trade")
                                    continue
                                
                                # Track symbol enhancement 
                                symbol_enhanced = False
                                original_symbol = None
                                symbol = processed_trade.get('symbol', '')
                                
                                # Check if the symbol was already enhanced by the broker parser
                                if processed_trade.get('symbol_enhanced', False):
                                    symbol_enhancement_count += 1
                                    original_symbol = processed_trade.get('original_symbol', '')
                                    print(f"Enhanced {original_symbol} to {symbol} using {broker_type} processor")
                                    symbol_enhanced = True
                                
                                # Check for SPAC resolution if not already handled
                                if not processed_trade.get('symbol_resolved', False) and symbol:
                                    description = processed_trade.get('description', '')
                                    try:
                                        # Ensure symbol is clean before lookup
                                        clean_symbol = ''.join(symbol.strip().split())
                                        
                                        # Use our SPAC resolution tools
                                        orig, resolved = lookup_spac_merger(clean_symbol, description)
                                        if orig != resolved:
                                            # We have a SPAC symbol that was resolved
                                            if not original_symbol:
                                                original_symbol = clean_symbol
                                            processed_trade['original_symbol'] = original_symbol
                                            processed_trade['symbol'] = resolved
                                            processed_trade['symbol_resolved'] = True
                                            symbol_enhanced = True
                                            symbol_enhancement_count += 1
                                            print(f"SPAC symbol resolved at upload: {original_symbol} → {resolved}")
                                            
                                            # Get detailed info about the SPAC merger
                                            try:
                                                spac_info = search_spac_info_with_llm(original_symbol, description)
                                                if spac_info and spac_info.get("merger_status") == "completed":
                                                    merger_info = f"{original_symbol} → {spac_info.get('current_symbol')} "
                                                    if spac_info.get('target_company'):
                                                        merger_info += f"(merged with {spac_info.get('target_company')}"
                                                        if spac_info.get('merger_date'):
                                                            merger_info += f" on {spac_info.get('merger_date')}"
                                                        merger_info += ")"
                                                    
                                                    print(f"SPAC info: {merger_info}")
                                                    
                                                    # Add merger info to the trade description
                                                    merger_note = f" [SPAC merger: {merger_info}]"
                                                    if processed_trade.get('description'):
                                                        processed_trade['description'] += merger_note
                                            except Exception as spac_error:
                                                print(f"Error getting SPAC info: {spac_error}")
                                    except Exception as spac_err:
                                        print(f"Error in SPAC resolution: {spac_err}")
                                
                                # Final clean-up of symbol to remove any spaces
                                if processed_trade.get('symbol'):
                                    processed_trade['symbol'] = ''.join(processed_trade['symbol'].strip().split())
                                
                                # Extract key fields for the Trade object
                                symbol = processed_trade.get('symbol', '')
                                quantity = float(processed_trade.get('quantity', 0))
                                price = float(processed_trade.get('price', 0))
                                side = processed_trade.get('side', 'BUY')
                                description = processed_trade.get('description', '')
                                
                                # Get date from processed data or use current date
                                trade_date = processed_trade.get('date', datetime.now().strftime('%Y-%m-%d'))
                                if isinstance(trade_date, datetime):
                                    trade_date = trade_date.strftime('%Y-%m-%d')
                                    
                                # Extract other trade details
                                commission = float(processed_trade.get('commission', 0.0))
                                net_proceeds = float(processed_trade.get('net_proceeds', 0.0))
                                is_option = processed_trade.get('is_option', False)
                                
                                # Extract option-specific details
                                option_type = processed_trade.get('option_type')
                                strike_price = processed_trade.get('strike_price')
                                expiry_date = processed_trade.get('expiry_date')
                                
                                # Convert expiry_date from string to datetime if needed
                                if expiry_date and isinstance(expiry_date, str):
                                    try:
                                        # Convert to date object first, then back to datetime with time at 00:00:00
                                        date_only = datetime.strptime(expiry_date, '%Y-%m-%d').date()
                                        expiry_date = datetime.combine(date_only, datetime.min.time())
                                    except ValueError:
                                        # If conversion fails, log warning but continue
                                        print(f"Warning: Could not convert expiry_date '{expiry_date}' to datetime")
                                elif expiry_date and isinstance(expiry_date, datetime):
                                    # Ensure the time component is zeroed out
                                    date_only = expiry_date.date()
                                    expiry_date = datetime.combine(date_only, datetime.min.time())
                                
                                dte = processed_trade.get('dte')
                                
                                # Get time from processed data
                                time = processed_trade.get('time')
                                
                                # Initialize trade_timestamp from the date
                                try:
                                    trade_timestamp = datetime.strptime(trade_date, '%Y-%m-%d')
                                except ValueError:
                                    trade_timestamp = datetime.now()
                                
                                # If timestamp exists in processed_trade, use it instead of creating from date
                                if processed_trade.get('timestamp') and isinstance(processed_trade['timestamp'], datetime):
                                    trade_timestamp = processed_trade['timestamp']
                                
                                # Create the trade object for the database
                                trade = Trade(
                                    user_id=user.id,
                                    symbol=symbol,
                                    quantity=abs(quantity),
                                    price=price,
                                    timestamp=trade_timestamp,
                                    date=trade_date,
                                    time=time,
                                    side=side,
                                    status=TradeStatus.COMPLETED,
                                    broker_type=broker_type,
                                    commission=commission,
                                    net_proceeds=net_proceeds,
                                    is_option=is_option,
                                    option_type=option_type,
                                    strike_price=strike_price,
                                    expiry_date=expiry_date,
                                    dte=dte,
                                    description=description
                                )
                                
                                session.add(trade)
                                trades.append({
                                    "symbol": trade.symbol,
                                    "original_symbol": processed_trade.get('original_symbol'),
                                    "quantity": trade.quantity,
                                    "price": trade.price,
                                    "side": trade.side,
                                    "date": trade.date,
                                    "timestamp": str(trade.timestamp),
                                    "expiry_date": trade.expiry_date.strftime('%Y-%m-%d') if trade.expiry_date else None,
                                    "description": description[:50] + "..." if description and len(description) > 50 else description
                                })
                            except Exception as row_error:
                                error_count += 1
                                print(f"Error processing row {row_count}: {row}")
                                print(f"Error details: {str(row_error)}")
                                continue
                        
                        if row_count == 0:
                            print("Warning: CSV file has no data rows")
                            return jsonify({"error": "CSV file has no data rows", "success": False}), 400
                        
                        if len(trades) == 0:
                            print(f"Warning: No valid trades found in {row_count} rows")
                            return jsonify({"error": "No valid trades found in the CSV file", "success": False}), 400
                        
                        if error_count > 0:
                            print(f"Warning: {error_count} out of {row_count} rows had errors")
                        
                        # Commit all trades
                        session.commit()
                        print(f"Successfully committed {len(trades)} trades to the database")
                        
                        # Create a processed file with summary information
                        processed_filename = f"processed_{save_filename}"
                        processed_path = os.path.join(UPLOADS_DIR, processed_filename)
                        
                        with open(processed_path, 'w') as f:
                            f.write(f"Original file: {original_filename}\n")
                            f.write(f"Processed on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                            f.write(f"User: {user_email}\n")
                            f.write(f"Broker: {broker_type}\n")
                            f.write(f"Trades processed: {len(trades)}\n")
                            f.write(f"Rows with errors: {error_count}\n")
                            if broker_parser.use_symbol_enhancement or symbol_enhancement_count > 0:
                                f.write(f"Symbols enhanced: {symbol_enhancement_count}\n")
                            
                            # Add SPAC information if any were detected
                            resolved_spacs = sum(1 for t in trades if t.get('is_spac', False))
                            if resolved_spacs > 0 or potential_spacs > 0:
                                f.write(f"Resolved SPACs: {resolved_spacs}\n")
                                f.write(f"Potential unresolved SPACs: {potential_spacs}\n")
                            
                            f.write("\n")
                            f.write("Symbol,OriginalSymbol,Quantity,Price,Side,Date,Description,IsSPAC\n")
                            
                            for trade in trades:
                                original = trade.get('original_symbol', '')
                                desc = trade.get('description', '')
                                is_spac = "Yes" if trade.get('is_spac', False) or trade.get('potential_spac', False) else "No"
                                f.write(f"{trade['symbol']},{original},{trade['quantity']},{trade['price']},{trade['side']},{trade['date']},{desc},{is_spac}\n")
                        
                        print(f"Saved processed file to: {processed_path}")
                        
                        response_data = {
                            "success": True,
                            "message": f"Successfully processed {len(trades)} trades from {broker_type}",
                            "trades": trades,
                            "saved_path": processed_filename,
                            "total_rows": row_count,
                            "error_rows": error_count,
                            "broker_format": broker_type
                        }
                        
                        # Only include enhanced_symbols if symbol enhancement was used
                        if broker_parser.use_symbol_enhancement or symbol_enhancement_count > 0:
                            response_data["enhanced_symbols"] = symbol_enhancement_count
                            response_data["symbol_enhancement_enabled"] = True
                            
                        # Count potential SPACs for reporting
                        potential_spacs = sum(1 for t in trades if t.get('potential_spac', False))
                        if potential_spacs > 0:
                            response_data["potential_spacs"] = potential_spacs
                            print(f"Flagged {potential_spacs} trades as potential SPACs for review")
                        
                        return jsonify(response_data)
                    except csv.Error as e:
                        print(f"CSV parsing error: {str(e)}")
                        return jsonify({"error": f"CSV parsing error: {str(e)}", "success": False}), 400
                except Exception as e:
                    error_msg = f"Error processing file: {str(e)}"
                    print(error_msg)
                    print(traceback.format_exc())
                    return jsonify({"error": error_msg, "success": False}), 500
            except Exception as e:
                error_msg = f"Database error: {str(e)}"
                print(error_msg)
                print(traceback.format_exc())
                return jsonify({"error": error_msg, "success": False}), 500
    except Exception as e:
        error_msg = f"Server error: {str(e)}"
        print(error_msg)
        print(traceback.format_exc())
        return jsonify({"error": error_msg, "success": False}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True) 