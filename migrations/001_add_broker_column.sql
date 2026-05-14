-- Add broker column to paper_positions and backfill existing records as 'alpaca'
-- Run this in the Supabase SQL Editor (Dashboard → SQL Editor → New Query)

ALTER TABLE paper_positions
ADD COLUMN IF NOT EXISTS broker TEXT NOT NULL DEFAULT 'alpaca';

-- Backfill: all existing records are from Alpaca
UPDATE paper_positions SET broker = 'alpaca' WHERE broker IS NULL OR broker = '';

-- Also add to paper_trade_events for event-level tracking
ALTER TABLE paper_trade_events
ADD COLUMN IF NOT EXISTS broker TEXT NOT NULL DEFAULT 'alpaca';

UPDATE paper_trade_events SET broker = 'alpaca' WHERE broker IS NULL OR broker = '';
