-- ES overnight return tracking for gap risk analysis.
-- Stores the ES futures return vs previous session close at time of position close.
ALTER TABLE paper_positions ADD COLUMN IF NOT EXISTS es_overnight_pct numeric;
