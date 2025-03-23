# Trade CSV Upload Application

This application allows users to upload CSV files from various brokers and stores the trade data in a PostgreSQL database. The application is built with Flask and SQLModel for Python 3.13 compatibility.

## Setup

1. Configure environment variables:
   - Copy the `.env.example` file to `.env` or create a new `.env` file
   - Update the database credentials and other settings as needed

2. Install Python dependencies:
```
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. Setup the PostgreSQL database (make sure PostgreSQL is installed and running):
```
python reset_db.py
```

## Running the Application

1. Start the server:
```
python app.py
```

2. Open your browser and navigate to `http://localhost:8080`

## Features

- User-friendly web interface for uploading CSV files
- Support for various broker formats
- Automatic user creation
- PostgreSQL database for data storage
- Responsive design
- Backup of uploaded CSV files in the `uploads` directory
- Processed CSV files with summary information
- Broker-specific AI-powered stock symbol enhancement using Gemini API
  - Only enabled for Fidelity, Interactive Brokers, and Robinhood
  - Automatically converts non-standard symbols (with numbers) to standard tickers
  - Uses security descriptions to identify the correct ticker when available

## CSV Format

The application expects CSV files with the following columns:
- Symbol: Stock ticker symbol (e.g., AAPL)
- Quantity: Number of shares
- Price: Price per share
- Side: Buy or Sell
- Date: Transaction date (optional, defaults to current date if missing)

Example:
```
Symbol,Quantity,Price,Side,Date
AAPL,10,190.50,Buy,2025-03-21
MSFT,5,425.75,Buy,2025-03-21
```

## File Storage

When a CSV file is uploaded:
1. The original file is saved to the `uploads` directory with a filename format: `timestamp_email_broker_originalfilename.csv`
2. A processed version of the file is created with a prefix `processed_` that includes:
   - A summary header with metadata
   - The processed trades in CSV format

This ensures you have both the original file and a record of what was processed.

## Database Models

The application uses SQLModel to define database models:

### User Model
- id: Primary key
- email: User's email address
- name: User's name
- password: User's password
- created_at: User creation timestamp
- updated_at: User update timestamp

### Trade Model
- id: Primary key
- user_id: Foreign key to User
- timestamp: Date and time of the trade
- date: Date of the trade (YYYY-MM-DD)
- time: Time of the trade (optional)
- symbol: Stock symbol
- price: Price per share
- quantity: Number of shares
- side: BUY or SELL
- status: Trade status (COMPLETED, PENDING, CANCELLED)
- commission: Commission fee
- net_proceeds: Net proceeds from trade
- broker_type: Type of broker
- is_option: Whether it's an options trade
- option_type: Type of option (for option trades)
- strike_price: Strike price (for option trades)
- expiry_date: Expiration date (for option trades)
- description: Trade description
- dte: Days to expiration (for option trades)

## API Endpoints

- `GET /` - Main web interface
- `POST /upload` - Upload and process a CSV file
- `POST /users` - Create a new user
- `GET /users` - Get a list of users

## Technology Stack

- Flask: Web framework
- SQLModel: Database ORM
- PostgreSQL: Database
- HTML/CSS/JavaScript: Frontend

## Supported Brokers

The application supports the following brokers:
- Fidelity
- Interactive Brokers
- Robinhood
- Charles Schwab
- TastyTrade
- TradingView
- Webull

## Development

To run the server in development mode with auto-reload:
```
uvicorn app:app --reload
```

To reset the database:
```
python reset_db.py
```

To test if the server is running correctly:
```
curl http://localhost:8080/docs
```

## API Endpoints

- `GET /api/trades` - Get a list of trades
- `GET /api/trades/by-date` - Get trades by date
- `GET /api/trades/by-broker/:broker` - Get trades by broker
- `GET /api/trades/by-symbol/:symbol` - Get trades by symbol
- `GET /api/market-data/:symbol` - Get market data for a symbol 