#!/usr/bin/env python3

from sqlmodel import SQLModel, Session, delete
from flask_app import Trade
from database import engine

def delete_all_trades():
    """Delete all trades from the database"""
    with Session(engine) as session:
        # Delete all records from the Trade table
        session.exec(delete(Trade))
        session.commit()
        print("All trades deleted successfully")

if __name__ == "__main__":
    delete_all_trades() 