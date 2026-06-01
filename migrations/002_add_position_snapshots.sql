-- 3:50 PM ET position snapshots for overnight hold analysis
-- Run this in the Supabase SQL Editor (Dashboard → SQL Editor → New Query)

CREATE TABLE IF NOT EXISTS position_snapshots (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT now(),
    snapshot_date DATE NOT NULL,
    order_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    direction TEXT,
    option_type TEXT,
    strike FLOAT,
    expiry TEXT,
    filled_price FLOAT,
    current_price FLOAT,
    pnl_pct FLOAT,
    peak_pnl_pct FLOAT,
    held_hours FLOAT,
    broker TEXT NOT NULL DEFAULT 'ib'
);

CREATE INDEX IF NOT EXISTS idx_snapshots_date ON position_snapshots(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_snapshots_order ON position_snapshots(order_id);
