import os
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import time
from dotenv import load_dotenv

load_dotenv()

# --- Database Connection ---
def get_db_connection():
    """Establishes a connection to the PostgreSQL database using credentials from environment variables."""
    try:
        db_user = os.environ["DB_USER"]
        db_password = os.environ["DB_PASSWORD"]
        db_host = os.environ["DB_HOST"]
        db_port = os.environ["DB_PORT"]
        db_name = os.environ["DB_NAME"]
        
        connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)
        return engine
    except KeyError as e:
        st.error(f"Environment variable not set: {e}. Please configure the database credentials in your Databricks App settings.")
        return None
    except Exception as e:
        st.error(f"Failed to connect to the database: {e}")
        return None

def setup_database(engine):
    """Creates the 'entries' table if it doesn't exist."""
    try:
        with engine.connect() as connection:
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS entries (
                    id SERIAL PRIMARY KEY,
                    content TEXT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """))
            connection.commit()
    except SQLAlchemyError as e:
        st.error(f"Error during table setup: {e}")


# --- Data Fetching ---
def get_entries(engine):
    """Fetches all entries from the 'entries' table."""
    try:
        with engine.connect() as connection:
            df = pd.read_sql("SELECT id, content, created_at FROM entries ORDER BY created_at DESC", connection)
            return df
    except SQLAlchemyError as e:
        st.error(f"Error fetching entries: {e}")
        return pd.DataFrame()

# --- Data Writing ---
def add_entry(engine, content):
    """Adds a new entry to the 'entries' table."""
    try:
        with engine.connect() as connection:
            connection.execute(text("INSERT INTO entries (content) VALUES (:content)"), {"content": content})
            connection.commit()
        return True
    except SQLAlchemyError as e:
        st.error(f"Error adding entry: {e}")
        return False

# --- Streamlit App ---
st.set_page_config(layout="wide", page_title="PostgreSQL Data Editor")

st.title("📝 PostgreSQL Data Editor")
st.markdown("""
This application connects to a PostgreSQL database managed by Databricks.
You can view existing entries and add new ones using the form below.
""")

engine = get_db_connection()

if engine:
    setup_database(engine)

    # --- Display Entries ---
    st.header("Current Entries")
    entries_df = get_entries(engine)
    st.dataframe(entries_df, use_container_width=True)

    # --- Add New Entry Form ---
    st.header("Add a New Entry")
    with st.form("new_entry_form", clear_on_submit=True):
        entry_content = st.text_area("Content", placeholder="Enter your text here...")
        submitted = st.form_submit_button("Add Entry")

        if submitted:
            if entry_content:
                if add_entry(engine, entry_content):
                    st.success("Entry added successfully!")
                    time.sleep(1) # Brief pause to let the user see the message
                    st.rerun()
                else:
                    st.error("Failed to add the entry.")
            else:
                st.warning("Please enter some content.") 