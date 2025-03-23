#!/usr/bin/env python3
import requests
import re
import os
import logging
import json
from typing import Optional, Dict, List, Tuple, Any
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('symbol_enhancer')

# Load environment variables
load_dotenv()

# Cache for stock symbol lookups to reduce API calls
SYMBOL_CACHE: Dict[str, str] = {}
symbol_cache_file = os.path.join(os.path.dirname(__file__), 'symbol_cache.json')

# Load cache if it exists
if os.path.exists(symbol_cache_file):
    try:
        with open(symbol_cache_file, 'r') as f:
            SYMBOL_CACHE = json.load(f)
        logger.info(f"Loaded {len(SYMBOL_CACHE)} entries from symbol cache")
    except Exception as e:
        logger.error(f"Error loading symbol cache: {e}")
        SYMBOL_CACHE = {}

# Common company suffixes to ignore when extracting tickers
COMMON_SUFFIXES = [
    'INC', 'CORP', 'LTD', 'CO', 'LLC', 'HOLDINGS', 'GROUP', 'PLC', 'TECHNOLOGY', 
    'TECHNOLOGIES', 'INTERNATIONAL', 'ETF', 'FUND', 'TRUST', 'REIT', 'BANCORP',
    'ADR', 'ADS', 'LP', 'SA', 'AG', 'SE', 'NV', 'LTD', 'PTE', 'BHD', 'BERHAD',
    'HK', 'KK', 'OYJ', 'ASA', 'OY', 'AB', 'GMBH', 'HLDGS', 'HLDG'
]

# Common words to ignore
COMMON_WORDS = [
    'THE', 'AND', 'OF', 'FOR', 'IN', 'ON', 'BY', 'WITH', 'TO', 'A', 'AN', 
    'FROM', 'CLASS', 'SERIES', 'CL', 'SER', 'COMMON', 'COM', 'STOCK', 'SHARE',
    'SHARES', 'NEW', 'EACH', 'USD', 'CASH', 'EXCHANGE', 'TRADED', 'MONEY', 'MARKET',
    'EACH', 'REPRESENTS', 'REPRESENTING', 'PAR', 'VALUE', 'ORDINARY', 'PREFERRED', 'DEPOSITARY',
    'RECEIPT', 'RECEIPTS'
]

# SPAC indicators
SPAC_INDICATORS = [
    'SPAC', 'ACQUISITION', 'BLANK CHECK', 'SPECIAL PURPOSE', 'CAPITAL', 'PARTNERS',
    'MERGER', 'SPONSOR', 'UNIT', 'WARR', 'WTS', 'UNITS'
]

# Gemini API configuration from environment variables
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_API_URL = os.getenv("GEMINI_API_URL", "https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-2:generateContent")
# Try the flash model if the main model is not available
GEMINI_BACKUP_URL = os.getenv("GEMINI_BACKUP_URL", "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent")

def extract_ticker_candidates(description: str) -> List[str]:
    """
    Extract potential ticker symbols from a description.
    
    Args:
        description: The security description
        
    Returns:
        List of potential ticker symbols
    """
    if not description:
        return []
    
    # Convert to uppercase for consistency
    description = description.upper()
    
    # First, look for patterns that are likely to be tickers
    # Pattern 1: Words in parentheses - often contains the ticker
    paren_matches = re.findall(r'\(([A-Z]{1,5})\)', description)
    if paren_matches:
        return [m for m in paren_matches if m not in COMMON_SUFFIXES and m not in COMMON_WORDS]
    
    # Split the description into words
    words = re.findall(r'\b[A-Z0-9]{1,5}\b', description)
    
    # Filter out common words and suffixes
    candidates = [
        word for word in words 
        if word.isalpha() and len(word) <= 5 
        and word not in COMMON_SUFFIXES 
        and word not in COMMON_WORDS
    ]
    
    # Get the first word if it looks like a ticker (often the case for simple descriptions)
    if words and words[0].isalpha() and len(words[0]) <= 5:
        candidates.insert(0, words[0])
    
    # For SPACs, try to extract more meaningful parts
    if any(spac in description for spac in SPAC_INDICATORS):
        # Try to extract the main part of the SPAC name
        for word in words:
            if (word.isalpha() and 2 <= len(word) <= 5 and 
                word not in COMMON_SUFFIXES and 
                word not in COMMON_WORDS and
                word not in SPAC_INDICATORS):
                if word not in candidates:
                    candidates.append(word)
    
    # Handle special cases for well-known companies
    if "VIRGIN GALACTIC" in description:
        candidates.insert(0, "SPCE")
    elif "MINERCO" in description:
        candidates.insert(0, "MINE")
    elif "AIRNET TECHNOLOGY" in description:
        candidates.insert(0, "ANTE")
    elif "REINVENT TECHNOLOGY PARTNERS" in description:
        candidates.insert(0, "RTP")  # Original SPAC ticker before merger
    
    # Deduplicate while preserving order
    seen = set()
    unique_candidates = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            unique_candidates.append(c)
    
    return unique_candidates

def lookup_stock_symbol(potential_symbol: str, description: Optional[str] = None) -> str:
    """
    Looks up the standard stock ticker symbol using Gemini API or direct extraction.
    
    Args:
        potential_symbol: The potential symbol that might need enhancement
        description: Optional description of the security for better context
        
    Returns:
        The standard ticker symbol (enhanced) or the original symbol if enhancement failed
    """
    # Simple preprocessing of the input symbol
    if not potential_symbol:
        return ""
    
    potential_symbol = potential_symbol.strip().upper()
    
    # Handle common cases for money market and cash
    if "MONEY MARKET" in potential_symbol or "SPAXX" in potential_symbol or "CASH" in potential_symbol:
        return "SPAXX"
    
    # Check cache first
    cache_key = f"{potential_symbol}:{description or ''}"
    if cache_key in SYMBOL_CACHE:
        logger.info(f"Cache hit for {cache_key}")
        return SYMBOL_CACHE[cache_key]
    
    # If the potential symbol already looks like a valid ticker, use it
    if not any(c.isdigit() for c in potential_symbol) and potential_symbol.isalpha() and len(potential_symbol) <= 5:
        logger.info(f"Symbol {potential_symbol} already looks valid, using as-is")
        SYMBOL_CACHE[cache_key] = potential_symbol
        return potential_symbol
    
    # Handle special cases with known mappings
    if potential_symbol in ["00941Q104"]:
        logger.info(f"Using known mapping for {potential_symbol} -> ANTE")
        SYMBOL_CACHE[cache_key] = "ANTE"
        return "ANTE"
    elif potential_symbol in ["603171109"]:  
        logger.info(f"Using known mapping for {potential_symbol} -> MINE")
        SYMBOL_CACHE[cache_key] = "MINE"
        return "MINE"
    elif potential_symbol in ["G7483N129"]:
        logger.info(f"Using known mapping for {potential_symbol} -> RTP")  # Reinvent Technology Partners
        SYMBOL_CACHE[cache_key] = "RTP"
        return "RTP"
    elif potential_symbol in ["92766K106"]:
        logger.info(f"Using known mapping for {potential_symbol} -> SPCE")  # Virgin Galactic
        SYMBOL_CACHE[cache_key] = "SPCE"
        return "SPCE"
    
    # Try to extract symbol from CUSIP-like format
    if ":" in potential_symbol:
        parts = potential_symbol.split(":")
        if len(parts) >= 2:
            # Often format is "CUSIP:DESCRIPTION" - try to extract from description
            candidates = extract_ticker_candidates(parts[1])
            if candidates:
                potential_ticker = candidates[0]
                logger.info(f"Extracted ticker {potential_ticker} from {potential_symbol}")
                SYMBOL_CACHE[cache_key] = potential_ticker
                return potential_ticker
    
    # Try to extract from description if symbol looks like a CUSIP
    if description and (any(c.isdigit() for c in potential_symbol) or len(potential_symbol) > 5):
        # Extract ticker candidates from description
        candidates = extract_ticker_candidates(description)
        if candidates:
            logger.info(f"Found potential ticker {candidates[0]} in description for {potential_symbol}")
            SYMBOL_CACHE[cache_key] = candidates[0]
            return candidates[0]
    
    # Check if API key is available
    if not GEMINI_API_KEY:
        logger.warning(f"GEMINI_API_KEY not set. Symbol enhancement is disabled.")
        # Try to clean the symbol as a fallback
        cleaned = clean_symbol(potential_symbol)
        logger.info(f"Cleaned {potential_symbol} to {cleaned} without API")
        SYMBOL_CACHE[cache_key] = cleaned
        return cleaned
    
    # Prepare prompt for Gemini
    prompt = f"I need the standard stock ticker symbol for this security. "
    
    if description:
        prompt += f"The description is: '{description}'. "
    
    prompt += f"The potential symbol is: '{potential_symbol}'. "
    prompt += "Note: If this is a dissolved company, SPAC that merged, or otherwise inactive security, provide its last known ticker symbol. "
    prompt += "Reply with ONLY the standard ticker symbol (1-5 letters, no digits). If you can't determine it, reply with 'UNKNOWN'."
    
    logger.info(f"Querying Gemini for symbol: {potential_symbol}")
    
    headers = {
        "Content-Type": "application/json"
    }
    
    data = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 10
        }
    }
    
    # First try main model
    api_result = call_gemini_api(GEMINI_API_URL, headers, data)
    
    # If main model fails, try backup model
    if not api_result:
        logger.info(f"Trying backup model for {potential_symbol}")
        api_result = call_gemini_api(GEMINI_BACKUP_URL, headers, data)
    
    # Process API result if we got one
    if api_result:
        # If the API returned a valid-looking ticker
        if api_result != 'UNKNOWN' and api_result.isalpha() and len(api_result) <= 5:
            logger.info(f"Gemini identified {potential_symbol} as {api_result}")
            SYMBOL_CACHE[cache_key] = api_result
            return api_result
    
    # If we get here, we couldn't enhance the symbol with Gemini
    # Fallback to direct extraction from description
    if description:
        candidates = extract_ticker_candidates(description)
        if candidates:
            logger.info(f"Falling back to direct extraction for {potential_symbol}: {candidates[0]}")
            SYMBOL_CACHE[cache_key] = candidates[0]
            return candidates[0]
    
    # Last resort: clean the symbol
    cleaned = clean_symbol(potential_symbol)
    logger.info(f"Cleaned {potential_symbol} to {cleaned} as last resort")
    SYMBOL_CACHE[cache_key] = cleaned
    return cleaned

def call_gemini_api(api_url, headers, data):
    """Helper function to call Gemini API and handle response"""
    try:
        # Make API request to Gemini
        logger.info(f"Sending request to Gemini API: {api_url}")
        response = requests.post(
            f"{api_url}?key={GEMINI_API_KEY}",
            headers=headers,
            json=data
        )
        
        # Log response status
        logger.info(f"Gemini API response status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            logger.debug(f"Full Gemini response: {result}")
            
            if 'candidates' in result and len(result['candidates']) > 0:
                text = result['candidates'][0]['content']['parts'][0]['text'].strip()
                # Remove any quotes or extra characters
                text = text.replace('"', '').replace("'", "").strip()
                return text
            else:
                logger.warning(f"No candidates in Gemini response")
        else:
            logger.error(f"Gemini API error {response.status_code}: {response.text}")
        
        return None
        
    except Exception as e:
        logger.error(f"Error calling Gemini API: {str(e)}")
        return None

def needs_enhancement(symbol: str) -> bool:
    """
    Determines if a symbol needs enhancement.
    
    Args:
        symbol: The stock symbol to check
        
    Returns:
        True if the symbol contains digits or is longer than 5 characters
    """
    if not symbol:
        return False
        
    symbol = symbol.strip()
    
    # Special cases that don't need enhancement
    if symbol in ['SPAXX']:
        return False
        
    return bool(any(c.isdigit() for c in symbol) or len(symbol) > 5 or ":" in symbol)

def clean_symbol(symbol: str) -> str:
    """
    Performs basic cleaning on a symbol without using the API.
    
    Args:
        symbol: The stock symbol to clean
        
    Returns:
        A cleaned version of the symbol
    """
    if not symbol:
        return ""
        
    # Check for CUSIP-like format with description
    if ":" in symbol:
        parts = symbol.split(":")
        if len(parts) >= 2:
            # Extract words from description part
            candidates = extract_ticker_candidates(parts[1])
            if candidates:
                return candidates[0]
    
    # Known CUSIP mappings
    if symbol == "00941Q104":
        return "ANTE"
    elif symbol == "603171109":
        return "MINE"
    elif symbol == "G7483N129":
        return "RTP"
    elif symbol == "92766K106":
        return "SPCE"
        
    # Remove any non-alphabetic characters
    clean = re.sub(r'[^A-Za-z]', '', symbol)
    
    # Take the first 5 characters (max symbol length)
    if len(clean) > 5:
        clean = clean[:5]
        
    return clean.upper() if clean else symbol 

def save_cache():
    """Save the symbol cache to disk"""
    try:
        with open(symbol_cache_file, 'w') as f:
            json.dump(SYMBOL_CACHE, f)
        logger.debug(f"Saved {len(SYMBOL_CACHE)} entries to symbol cache")
    except Exception as e:
        logger.error(f"Error saving symbol cache: {e}")

def identify_potential_spac(description: str) -> bool:
    """
    Determine if a description likely refers to a SPAC based on common keywords and patterns
    
    Args:
        description: The security description text
        
    Returns:
        True if the description likely refers to a SPAC
    """
    if not description:
        return False
        
    # Convert to uppercase for case-insensitive matching
    description_upper = description.upper()
    
    # Common SPAC keywords and patterns
    spac_keywords = [
        "SPAC", 
        "ACQUISITION CORP", 
        "ACQUISITION HOLDINGS",
        "CAPITAL CORP", 
        "HOLDINGS CORP", 
        "MERGER",
        "SPECIAL PURPOSE",
        "BLANK CHECK",
        "TECHNOLOGY PARTNERS",
        "NEXTGEN ACQUISITION",
        "CAPITAL INVESTMENT",
        "UNIT",  # SPACs often trade as units initially
        "WARRANT",  # SPACs have warrants
        "CLASS A",  # SPACs often have Class A shares
        "CL A"  # Abbreviated Class A
    ]
    
    # Check for known SPAC sponsors
    spac_sponsors = [
        "CHAMATH",
        "SOCIAL CAPITAL",
        "PERSHING SQUARE",
        "DIAMOND EAGLE",
        "CHURCHILL CAPITAL",
        "VECTOR ACQUISITION",
        "REINVENT TECH",
        "ATLAS CREST",
        "HORIZON ACQUISITION",
        "SOFTBANK"
    ]
    
    # Check if any SPAC keyword is in the description
    for keyword in spac_keywords:
        if keyword in description_upper:
            return True
            
    # Check if any SPAC sponsor is in the description
    for sponsor in spac_sponsors:
        if sponsor in description_upper:
            return True
            
    return False

def lookup_spac_merger(symbol: str, description: str = None) -> Tuple[str, str]:
    """
    Look up SPAC symbol mappings by searching the web.
    Returns a tuple of (current_symbol, post_merger_symbol)
    
    In a production environment, this would call an LLM API to search the web
    for SPAC merger information and return the current correct ticker.
    """
    # First, thoroughly clean the symbol of any spaces or whitespace
    if not symbol:
        return (symbol, symbol)
        
    # Aggressively strip all whitespace from the symbol
    cleaned_symbol = ''.join(symbol.split())
    
    # Check cache first to avoid repeated lookups
    cache_key = f"SPAC_{cleaned_symbol}"
    if cache_key in SYMBOL_CACHE:
        logger.info(f"Cache hit for SPAC lookup {cleaned_symbol}: {SYMBOL_CACHE[cache_key]}")
        return (symbol, SYMBOL_CACHE[cache_key])
    
    # PLACEHOLDER: In production, here we would:
    # 1. Call an LLM API with a prompt like:
    #    "What is the current ticker symbol for the SPAC that was formerly known as {symbol}?
    #    If it underwent a merger, what is its post-merger ticker? Extract just the symbols."
    # 2. Parse the LLM response to get the current ticker
    
    # Static mapping for common SPACs - would be replaced with LLM lookup in production
    spac_mappings = {
        "IPOA": "SPCE",  # Social Capital Hedosophia → Virgin Galactic
        "IPOB": "OPEN",  # Social Capital Hedosophia II → Opendoor
        "IPOC": "CLOV",  # Social Capital Hedosophia III → Clover Health
        "IPOD": "IPOD",  # Social Capital Hedosophia IV (no merger completed)
        "IPOE": "SOFI",  # Social Capital Hedosophia V → SoFi
        "IPOF": "IPOF",  # Social Capital Hedosophia VI (no merger completed)
        "CCIV": "LCID",  # Churchill Capital IV → Lucid Motors
        "PSTH": "PSTH",  # Pershing Square Tontine Holdings (no merger completed)
        "VTIQ": "NKLA",  # VectoIQ → Nikola
        "SPAQ": "FSRW",  # Spartan Acquisition → Fisker
        "DKNG": "DKNG",  # Diamond Eagle Acquisition → DraftKings
        "DEAC": "DKNG",  # Diamond Eagle Acquisition → DraftKings (pre-ticker change)
        "RTP": "JOBY",   # Reinvent Technology Partners → Joby Aviation
        "RTPY": "AURA",  # Reinvent Technology Partners Y → Aurora Innovation
        "ACIC": "JOBY",  # Atlas Crest Investment Corp → Joby Aviation (alternate SPAC)
        "HZON": "SPRT",  # Horizon Acquisition → Support.com/Greenidge
        "SFTW": "BKS",   # BlackSky Technology 
        "VACQ": "RKLB",  # Vector Acquisition → Rocket Lab
        "NGAC": "EMBK",  # NextGen Acquisition → Embark Trucks
    }
    
    if cleaned_symbol.upper() in spac_mappings:
        current_symbol = spac_mappings[cleaned_symbol.upper()]
        # Cache this result for future lookups
        SYMBOL_CACHE[cache_key] = current_symbol
        save_cache()
        return (symbol, current_symbol)
    
    # If we have a description, we can try to extract clues about the SPAC
    if description:
        # Look for company names in the description that might indicate what the SPAC merged with
        company_indicators = [
            # Format: (company name pattern, known ticker)
            ("VIRGIN GALACTIC", "SPCE"),
            ("LUCID", "LCID"),
            ("DRAFTKINGS", "DKNG"),
            ("OPENDOOR", "OPEN"),
            ("CLOVER HEALTH", "CLOV"),
            ("SOFI", "SOFI"),
            ("NIKOLA", "NKLA"),
            ("FISKER", "FSRW"),
            ("JOBY AVIATION", "JOBY"),
            ("JOBY", "JOBY"),
            ("AURORA INNOVATION", "AURA"),
            ("BLACKSKY", "BKS"),
            ("ROCKET LAB", "RKLB"),
            ("EMBARK", "EMBK"),
            # Add more as needed
        ]
        
        description_upper = description.upper() if description else ""
        for indicator, ticker in company_indicators:
            if indicator in description_upper:
                # Found a match in the description
                SYMBOL_CACHE[cache_key] = ticker
                save_cache()
                logger.info(f"SPAC lookup based on description: {cleaned_symbol} -> {ticker}")
                return (symbol, ticker)
                
        # Check if this is likely a SPAC using our dedicated function
        if identify_potential_spac(description):
            # Log this for future enhancement
            print(f"Potential SPAC detected: {cleaned_symbol} - {description}")
    
    # If all else fails, we couldn't determine a mapping, return the cleaned symbol
    return (symbol, cleaned_symbol)
    
def search_spac_info_with_llm(spac_symbol: str, description: str = None) -> Dict:
    """
    Uses an LLM to search the web for information about a SPAC and its merger status.
    
    In a production environment, this would:
    1. Call a web search API to find information about the SPAC
    2. Feed the search results to an LLM to extract structured information
    3. Return a dictionary with the current symbol, merger status, and target company
    
    This is a placeholder implementation demonstrating the structure.
    """
    # Check cache first
    cache_key = f"SPAC_LLM_{spac_symbol}"
    if cache_key in SYMBOL_CACHE:
        return json.loads(SYMBOL_CACHE[cache_key])
    
    # In a production implementation, this would call a web search API and an LLM
    # For demonstration, we'll implement a placeholder response for a few known SPACs
    
    # Example result structure that would be returned by an LLM
    result = {
        "original_symbol": spac_symbol,
        "current_symbol": spac_symbol,  # Default to same if unknown
        "merger_status": "unknown",
        "target_company": None,
        "merger_date": None,
        "source_urls": []
    }
    
    # Hardcoded examples for demonstration
    if spac_symbol.upper() == "IPOA":
        result = {
            "original_symbol": "IPOA",
            "current_symbol": "SPCE",
            "merger_status": "completed",
            "target_company": "Virgin Galactic",
            "merger_date": "2019-10-28",
            "source_urls": ["https://www.virgingalactic.com/articles/virgin-galactic-completes-merger-with-social-capital-hedosophia"]
        }
    elif spac_symbol.upper() == "CCIV":
        result = {
            "original_symbol": "CCIV",
            "current_symbol": "LCID",
            "merger_status": "completed",
            "target_company": "Lucid Motors",
            "merger_date": "2021-07-23",
            "source_urls": ["https://ir.lucidmotors.com/news-releases/news-release-details/lucid-motors-completes-business-combination-churchill-capital"]
        }
    
    # Cache the result
    SYMBOL_CACHE[cache_key] = json.dumps(result)
    save_cache()
    
    return result 

def extract_option_details(description: str, symbol: Optional[str] = None, 
                          trade_date: Optional[datetime] = None) -> Dict[str, Any]:
    """
    Extract option details from a description using pattern matching.
    
    Args:
        description: The option description
        symbol: Optional symbol for additional context
        trade_date: Optional trade date for calculating days to expiry
        
    Returns:
        Dictionary with option details (is_option, option_type, strike_price, expiry_date, dte)
    """
    if not description:
        return {
            "is_option": False,
            "option_type": None,
            "strike_price": None,
            "expiry_date": None,
            "dte": None
        }
        
    result = {
        "is_option": False,
        "option_type": None,
        "strike_price": None,
        "expiry_date": None,
        "dte": None
    }
    
    # First detect if this is an option
    option_keywords = ["PUT", "CALL", "OPTION", " C ", " P "]
    description_upper = description.upper()
    
    # Check if the description likely describes an option
    if any(keyword in description_upper for keyword in option_keywords) or description_upper.strip().endswith("P") or description_upper.strip().endswith("C"):
        result["is_option"] = True
        logger.info(f"Identified option: {description}")
        
        # Determine option type
        if "PUT" in description_upper or description_upper.strip().endswith("P"):
            result["option_type"] = "PUT"
        elif "CALL" in description_upper or description_upper.strip().endswith("C"):
            result["option_type"] = "CALL"
        
        # Look for strike price
        strike_price = None
        
        # Special case for Robinhood: check for dollar values after Call/Put - prioritize this pattern
        if strike_price is None or strike_price == 0 or strike_price >= 1000:  # Likely a year
            # First try to find dollar amounts after Call/Put - prioritize this pattern
            dollar_pattern = r'(?:Call|Put)\s+\$?(\d+(?:\.\d+)?)'
            dollar_match = re.search(dollar_pattern, description)
            if dollar_match:
                # We found a dollar amount, use this as the strike price
                strike_price = float(dollar_match.group(1))
            else:
                # Try the more specific pattern with dollar sign which often appears at the end
                dollar_sign_pattern = r'(?:Call|Put).*?\$(\d+(?:\.\d+)?)'
                dollar_sign_match = re.search(dollar_sign_pattern, description)
                if dollar_sign_match:
                    strike_price = float(dollar_sign_match.group(1))
                else:
                    # Next try to find dollar amounts anywhere
                    dollar_match = re.search(r'\$(\d+(?:\.\d+)?)', description)
                    if dollar_match:
                        strike_price = float(dollar_match.group(1))
                
                # Check for the ".00" pattern and try to extract number before Call/Put
                zero_pattern = r'(?:Call|Put)\s+\.00'
                if re.search(zero_pattern, description_upper):
                    # Try to extract a number before the Call/Put word
                    # Case 1: Look for a number directly before Call/Put (like "390 Call")
                    before_pattern = r'(\d+(?:\.\d+)?)\s+(?:Call|Put)'
                    matches = re.finditer(before_pattern, description_upper)
                    
                    # Get all matches since the one closest to Call/Put is most likely the strike
                    all_matches = list(matches)
                    if all_matches:
                        # Use the last match (closest to the Call/Put)
                        last_match = all_matches[-1]
                        num_val = float(last_match.group(1))
                        
                        # Make sure this isn't a date component
                        date_pattern = r'(\d{1,2})/(\d{1,2})/(\d{4})'
                        date_match = re.search(date_pattern, description_upper)
                        if date_match:
                            # Extract date components
                            month = date_match.group(1)
                            day = date_match.group(2)
                            year = date_match.group(3)
                            # Check if the number we found is not part of the date
                            if (num_val != float(month) and 
                                num_val != float(day) and 
                                num_val != float(year)):
                                strike_price = num_val
                        else:
                            # No date found, use the number we found
                            strike_price = num_val
                
                # Handle formats like "XOM 19JUL24 80 P" where the strike price is before the P
                if strike_price is None or strike_price == 0:
                    # Look for number followed by P or C at the end
                    cp_end_pattern = r'(\d+(?:\.\d+)?)\s*[PC]$'
                    cp_end_match = re.search(cp_end_pattern, description_upper.strip())
                    if cp_end_match:
                        strike_price = float(cp_end_match.group(1))
                    else:
                        # Look for numbers followed by P or C anywhere
                        cp_pattern = r'(\d+(?:\.\d+)?)\s*[PC]\b'
                        cp_matches = list(re.finditer(cp_pattern, description_upper))
                        if cp_matches:
                            # Use the last match as it's most likely the strike price
                            last_cp_match = cp_matches[-1]
                            strike_price = float(last_cp_match.group(1))
                
                # Check if we extracted a date in m/d/yyyy format
                date_pattern = r'(\d{1,2})/(\d{1,2})/(\d{4})'
                date_match = re.search(date_pattern, description_upper)
                if date_match and strike_price is not None:
                    # We have a date that might be mistaken for a strike price
                    year = date_match.group(3)
                    if float(year) == strike_price:
                        # We mistook the year for a strike price, set it to 0
                        # and look for actual strike price in a different pattern
                        strike_price = 0
        
        result["strike_price"] = strike_price
        
        # Extract expiry date
        month_names = {
            "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
            "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
        }
        
        # Try various date patterns
        date_patterns = [
            # Format: mm/dd/yyyy or m/d/yyyy (Robinhood format)
            r'(\d{1,2})/(\d{1,2})/(\d{4})',
            # Format: DDMMMYY (19JUL25)
            r'(\d{1,2})([A-Z]{3})(\d{2})',
            # Format: DMMMYY (1JUL25)
            r'(\d)([A-Z]{3})(\d{2})',
            # Format: SPY JUL19 (month and day, no year specified)
            r'([A-Z]{3})(\d{1,2})',
            # Format: SPY JUL 19 (month and day with space, no year specified)
            r'([A-Z]{3})\s+(\d{1,2})',
            # Format: SPY 19JUL (day and month, no year specified)
            r'(\d{1,2})([A-Z]{3})',
            # Format: SPY JUL19'24 or SPY JUL19'2024 (with year)
            r'([A-Z]{3})(\d{1,2})\'(?:\d{2}|\d{4})',
            # Format: SPY JUL2024 (month and year)
            r'([A-Z]{3})(\d{4})',
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, description_upper)
            if match:
                # Handle mm/dd/yyyy format
                if pattern == r'(\d{1,2})/(\d{1,2})/(\d{4})':
                    month = int(match.group(1))
                    day = int(match.group(2))
                    year = int(match.group(3))
                    
                    try:
                        # Create datetime with date only (time at 00:00:00)
                        expiry_date = datetime(year, month, day, 0, 0, 0)
                        result["expiry_date"] = expiry_date
                        
                        # Calculate days to expiry if we have a trade date
                        if trade_date:
                            days_to_expiry = calculate_dte(trade_date, expiry_date)
                            result["dte"] = days_to_expiry
                        break
                    except ValueError:
                        continue
                # Handle DDMMMYY format (19JUL25)
                elif len(match.groups()) == 3 and len(match.group(3)) == 2:
                    # Format is DDMMMYY or DMMMYY
                    day = int(match.group(1))
                    month_str = match.group(2)
                    year_str = match.group(3)
                    
                    if month_str in month_names:
                        month = month_names[month_str]
                        year = 2000 + int(year_str)  # Assume 20xx for 2-digit years
                        
                        try:
                            # Create datetime with date only (time at 00:00:00)
                            expiry_date = datetime(year, month, day, 0, 0, 0)
                            result["expiry_date"] = expiry_date
                            
                            # Calculate days to expiry if we have a trade date
                            if trade_date:
                                days_to_expiry = calculate_dte(trade_date, expiry_date)
                                result["dte"] = days_to_expiry
                            break
                        except ValueError:
                            # Handle case where day is invalid for month
                            try:
                                # Use last day of month
                                next_month = month + 1 if month < 12 else 1
                                next_year = year if month < 12 else year + 1
                                last_day = (datetime(next_year, next_month, 1) - timedelta(days=1)).day
                                day = min(day, last_day)
                                # Create datetime with date only (time at 00:00:00)
                                expiry_date = datetime(year, month, day, 0, 0, 0)
                                result["expiry_date"] = expiry_date
                                
                                # Calculate days to expiry if we have a trade date
                                if trade_date:
                                    days_to_expiry = calculate_dte(trade_date, expiry_date)
                                    result["dte"] = days_to_expiry
                                break
                            except (ValueError, TypeError):
                                continue
                elif len(match.groups()) >= 2:
                    # Determine if the format is month-day or day-month
                    first_group, second_group = match.groups()[0:2]
                    
                    # Check if first group is a month name
                    if first_group in month_names:
                        month = month_names[first_group]
                        day = int(second_group)
                    # Check if second group is a month name
                    elif second_group in month_names:
                        month = month_names[second_group]
                        day = int(first_group)
                    # If neither is a month name, continue to next pattern
                    else:
                        continue
                        
                    # Set a default year to current year
                    year = datetime.now().year
                    
                    # If month is earlier in the year than current month, use next year
                    if month < datetime.now().month:
                        year += 1
                        
                    try:
                        # Create datetime with date only (time at 00:00:00)
                        expiry_date = datetime(year, month, day, 0, 0, 0)
                        result["expiry_date"] = expiry_date
                        
                        # Calculate days to expiry if we have a trade date
                        if trade_date:
                            days_to_expiry = calculate_dte(trade_date, expiry_date)
                            result["dte"] = days_to_expiry
                        break
                    except ValueError:
                        # Handle case where day is invalid for month
                        try:
                            # Use last day of month
                            next_month = month + 1 if month < 12 else 1
                            next_year = year if month < 12 else year + 1
                            last_day = (datetime(next_year, next_month, 1) - timedelta(days=1)).day
                            day = min(day, last_day)
                            # Create datetime with date only (time at 00:00:00)
                            expiry_date = datetime(year, month, day, 0, 0, 0)
                            result["expiry_date"] = expiry_date
                            
                            # Calculate days to expiry if we have a trade date
                            if trade_date:
                                days_to_expiry = calculate_dte(trade_date, expiry_date)
                                result["dte"] = days_to_expiry
                            break
                        except (ValueError, TypeError):
                            continue
    
    return result

def calculate_dte(trade_date, expiry_date):
    """
    Calculate days to expiry based on trade date and expiry date.
    
    Args:
        trade_date: The date of the trade (datetime or string)
        expiry_date: The expiry date of the option (datetime)
        
    Returns:
        Integer representing days to expiry, or None if calculation not possible
    """
    if not trade_date or not expiry_date:
        return None
    
    # Make sure both are datetime objects
    if isinstance(trade_date, str):
        try:
            trade_date = datetime.strptime(trade_date, '%Y-%m-%d')
        except ValueError:
            return None
    
    # Calculate difference in days
    delta = expiry_date - trade_date
    return max(0, delta.days) 