# Getting Started (Local)

1) Install Python 3.10+ and ensure 'python' works in Command Prompt.
2) Create a virtual environment (recommended).
3) `pip install -r requirements.txt`
4) Copy `.env.example` to `.env` and fill PGHOST, PGUSER, etc.
5) Run: `python scripts\apply_schema.py` to create tables in your DB.
6) Run the app: `streamlit run app/streamlit_app.py`
