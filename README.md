# BI Agent for monday.com

A Python AI agent that answers business intelligence questions using **monday.com** data from your **Deals** and **Work Orders** boards. It cleans messy business data, runs pandas-based analysis, and returns **insights** (not just numbers) through a **Streamlit** conversational interface.

## Features

- **monday.com API** – Connects to your account and reads Deals and Work Orders boards
- **Data cleaning** – Handles missing values, normalizes sector names, parses inconsistent date formats
- **Natural-language Q&A** – Ask questions like:
  - *"How is our pipeline looking for the energy sector this quarter?"*
  - *"What is our total deal value?"*
  - *"Which sector is performing best?"*
- **Pandas analysis** – All metrics and breakdowns are computed with pandas
- **Insights** – Responses include short narrative insights and recommendations, not only raw numbers
- **Streamlit UI** – Chat-style interface with example questions

## Project structure

```
.
├── app.py              # Streamlit conversational interface
├── monday_client.py     # monday.com API client (Deals & Work Orders)
├── data_cleaning.py    # Cleaning: missing values, sectors, dates
├── agent.py            # BI agent: intent detection, pandas, insights
├── requirements.txt
└── README.md
```

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. monday.com API token and board IDs

- Create an API token in monday.com: **Developers** → **API** → **Personal API token** (or use an integration token).
- Find your **board IDs** in the board URL: `https://your-domain.monday.com/boards/1234567890` → board ID is `1234567890`.

Set these in your environment (or in a `.env` file and load with `python-dotenv`):

```bash
export MONDAY_API_KEY="your_api_token_here"
export MONDAY_DEALS_BOARD_ID="1234567890"
export MONDAY_WORK_ORDERS_BOARD_ID="0987654321"
```

On Windows (PowerShell):

```powershell
$env:MONDAY_API_KEY = "your_api_token_here"
$env:MONDAY_DEALS_BOARD_ID = "1234567890"
$env:MONDAY_WORK_ORDERS_BOARD_ID = "0987654321"
```

### 3. Run the app

```bash
streamlit run app.py
```

Open the URL shown in the terminal (e.g. `http://localhost:8501`). Click **Load from monday.com** in the sidebar to fetch and clean data, then ask your BI questions in the chat.

## Usage

1. **Load data** – In the sidebar, click **Load from monday.com**. The app will fetch Deals and Work Orders, clean them, and build the BI agent.
2. **Ask questions** – Type in the chat input. The agent detects intent (pipeline, total deal value, best sector), runs the right pandas analysis, and returns an insight.
3. **Example questions** – Use the expander at the bottom for suggested questions.

## Data cleaning (reference)

- **Missing values** – Rows with no name are dropped; missing sector is set to `"Other"`.
- **Sector normalization** – Common variants (e.g. "Oil & Gas", "Tech", "IT") are mapped to canonical names (e.g. "Energy", "Technology"). See `data_cleaning.SECTOR_NORMALIZATION`.
- **Dates** – Multiple formats are tried (e.g. `YYYY-MM-DD`, `DD/MM/YYYY`, `MM/DD/YYYY`, month names). Parsed dates are stored in `*_parsed` columns for filtering by quarter.

Column names are detected flexibly (e.g. "Sector" / "Industry", "Deal value" / "Value", "Create date" / "Close date"). If your boards use different titles, the cleaner will still try to match them.

## Configuration

- **API key** – Required; no default.
- **Board IDs** – Can be set via env vars (above) or passed when constructing `MondayClient(..., deals_board_id=..., work_orders_board_id=...)` in code.

## License

Use and modify as needed for your organization.
