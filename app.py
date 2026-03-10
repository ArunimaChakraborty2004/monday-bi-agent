"""
Streamlit app: conversational BI agent over monday.com Deals and Work Orders.

Run: streamlit run app.py
"""

import logging

import pandas as pd
import streamlit as st

from agent import BIAgent
from data_cleaning import load_and_clean_deals, load_and_clean_work_orders
from monday_client import MondayClient, MondayAPIError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="BI Agent | monday.com",
    page_icon="📊",
    layout="centered",
    initial_sidebar_state="expanded",
)

# Session state for chat history and data
if "messages" not in st.session_state:
    st.session_state.messages = []
if "deals_df" not in st.session_state:
    st.session_state.deals_df = None
if "work_orders_df" not in st.session_state:
    st.session_state.work_orders_df = None
if "agent" not in st.session_state:
    st.session_state.agent = None
if "data_loaded" not in st.session_state:
    st.session_state.data_loaded = False


def load_data_from_monday() -> tuple[bool, str]:
    """
    Fetch data from monday.com and clean it. Returns (success, message).
    """
    try:
        client = MondayClient()
        deals_raw = client.fetch_deals()
        work_orders_raw = client.fetch_work_orders()
    except ValueError as e:
        return False, str(e)
    except MondayAPIError as e:
        return False, f"API error: {e}"

    if not deals_raw and not work_orders_raw:
        return False, "No data returned from monday.com. Check board IDs and API key."

    st.session_state.deals_df = load_and_clean_deals(deals_raw) if deals_raw else None
    st.session_state.work_orders_df = (
        load_and_clean_work_orders(work_orders_raw) if work_orders_raw else None
    )
    st.session_state.agent = BIAgent(
        st.session_state.deals_df if st.session_state.deals_df is not None else pd.DataFrame(),
        st.session_state.work_orders_df if st.session_state.work_orders_df is not None else pd.DataFrame(),
    )
    st.session_state.data_loaded = True
    return True, "Data loaded successfully from monday.com."


def ensure_agent() -> BIAgent | None:
    """Return the BI agent if data is loaded; otherwise None."""
    if st.session_state.agent is not None:
        return st.session_state.agent
    if st.session_state.data_loaded and st.session_state.deals_df is not None:
        st.session_state.agent = BIAgent(
            st.session_state.deals_df,
            st.session_state.work_orders_df,
        )
        return st.session_state.agent
    return None


# Sidebar: data source and load
with st.sidebar:
    st.header("Data source")
    if st.button("Load from monday.com", use_container_width=True):
        with st.spinner("Fetching boards..."):
            ok, msg = load_data_from_monday()
            if ok:
                st.success(msg)
            else:
                st.error(msg)

    if st.session_state.data_loaded:
        deals_count = (
            len(st.session_state.deals_df)
            if st.session_state.deals_df is not None
            else 0
        )
        wo_count = (
            len(st.session_state.work_orders_df)
            if st.session_state.work_orders_df is not None
            else 0
        )
        st.caption(f"Deals: {deals_count} | Work orders: {wo_count}")

        with st.expander("Data diagnostics", expanded=False):
            deals_df = st.session_state.deals_df
            if deals_df is not None and not deals_df.empty:
                st.markdown("**Deals columns (sample):**")
                st.write(list(deals_df.columns)[:30])
                if "sector" in deals_df.columns:
                    st.markdown("**Top sectors:**")
                    st.write(deals_df["sector"].value_counts().head(10))
                if "deal_value_numeric" in deals_df.columns:
                    st.markdown("**Deal value numeric summary:**")
                    st.write(deals_df["deal_value_numeric"].describe())
                    st.caption(
                        f"Non-null deal values: {(deals_df['deal_value_numeric'].notna().sum())} / {len(deals_df)}"
                    )
                else:
                    st.warning(
                        "No `deal_value_numeric` column detected. Add a numeric/currency deal value column to the Deals board, "
                        "or ensure the column is visible and accessible via the API, then reload."
                    )
            else:
                st.caption("No Deals data loaded.")

    st.divider()
    st.caption(
        "Set **MONDAY_API_KEY**, **MONDAY_DEALS_BOARD_ID**, and "
        "**MONDAY_WORK_ORDERS_BOARD_ID** in your environment to load live data."
    )

# Main area: title and chat
st.title("📊 BI Agent")
st.markdown(
    "Ask questions about your **Deals** and **Work Orders** from monday.com. "
    "Load data from the sidebar first."
)

# Chat container
chat_container = st.container()
with chat_container:
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

# Prompt
if prompt := st.chat_input("Ask a BI question..."):
    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message("user"):
        st.markdown(prompt)

    agent = ensure_agent()
    if agent is None:
        response = (
            "**Load data first.** Click **Load from monday.com** in the sidebar. "
            "Make sure `MONDAY_API_KEY`, `MONDAY_DEALS_BOARD_ID`, and "
            "`MONDAY_WORK_ORDERS_BOARD_ID` are set in your environment."
        )
    else:
        response = agent.ask(prompt)

    st.session_state.messages.append({"role": "assistant", "content": response})

    with st.chat_message("assistant"):
        st.markdown(response)

# Example questions
with st.expander("Example questions"):
    st.markdown(
        """
        - How is our pipeline looking for the energy sector this quarter?
        - What is our total deal value?
        - Which sector is performing best?
        - Pipeline for technology this quarter
        """
    )
