# Zawadi Court Welfare System

This repo contains the Streamlit app and a managed-database (Supabase/Postgres) backend.

## What lives where
- `app/` — Streamlit app code
  - `persistence/` — database adapters (Supabase/Postgres), thin data-access layer
  - `pages/` — optional multipage structure (if you split the UI)
  - `assets/` — images, logos, icons
- `configs/` — configuration files (rates, constants, etc.); keep non-secrets here
- `db/` — SQL schema, seeds, and migrations (for Postgres/Supabase)
- `scripts/` — helper scripts (e.g., import from legacy CSV to Supabase)
- `tests/` — unit tests (optional)
- `docs/` — documentation, notes, checklists
- `data/legacy/` — your existing CSVs (local/dev only; the cloud disk is ephemeral)
- `.streamlit/` — Streamlit config + *secrets.toml* (for cloud deployment)

## Setup (local)
1. Create and activate a virtual environment (recommended).
2. `pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and fill values (local dev).
4. Copy `.streamlit/secrets.toml.example` to `.streamlit/secrets.toml` and fill values (for Streamlit Cloud).
5. Run app: `streamlit run app/streamlit_app.py`

## Production persistence
We will use **Supabase (Postgres)** as the system of record. CSVs are for legacy import/export only.
