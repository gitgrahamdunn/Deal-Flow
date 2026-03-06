# Deal Flow

See `deal_flow_ingest/README.md` for the ingestion MVP.

Apply saved SQL view files (when present):

```bash
python -m deal_flow_ingest.apply_saved_sql
```

Live ingestion note: AER ST37 is now the first real connected live source; Petrinex and liability connectors remain placeholder/live-skeleton until direct artifact URLs are wired.
