-- ChainSignal Supabase Schema
-- Run this in the Supabase SQL Editor (Dashboard > SQL Editor > New Query)

-- ==========================================================================
-- signals: every StrategyResult the system generates (live, scan, manual)
-- This is the primary accumulation table for forward-test data.
-- ==========================================================================
create table if not exists signals (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),

    -- core signal
    ticker text not null,
    direction text not null check (direction in ('BULLISH', 'BEARISH', 'NEUTRAL')),
    conviction text not null check (conviction in ('NONE', 'LOW', 'MEDIUM', 'HIGH', 'VERY_HIGH')),
    composite_score real not null,
    layers_aligned int not null default 0,

    -- context
    mode text not null check (mode in ('live', 'scan', 'manual', 'backtest')),
    regime text check (regime in ('BULLISH', 'BEARISH', 'NEUTRAL')),

    -- per-layer signals and scores
    flow_signal text,
    flow_score real,
    darkpool_signal text,
    darkpool_score real,
    gex_signal text,
    gex_score real,
    iv_signal text,
    iv_score real,
    technicals_signal text,
    technicals_score real,
    catalyst_signal text,
    catalyst_score real,
    social_signal text,
    social_score real,

    -- extras
    live_enhancements jsonb default '[]'::jsonb,
    raw_result jsonb
);

create index if not exists idx_signals_ticker on signals (ticker);
create index if not exists idx_signals_created_at on signals (created_at);
create index if not exists idx_signals_mode on signals (mode);
create index if not exists idx_signals_direction on signals (direction);

-- ==========================================================================
-- backtest_runs: metadata for each backtest execution
-- ==========================================================================
create table if not exists backtest_runs (
    id uuid primary key default gen_random_uuid(),
    created_at timestamptz not null default now(),

    start_date text not null,
    end_date text not null,
    top_n int not null,
    min_conviction text not null,
    delay real not null,

    -- summary stats
    total_trades int,
    win_rate real,
    profit_factor real,
    sharpe real,
    max_drawdown real,
    total_return real
);

-- ==========================================================================
-- backtest_trades: individual trades from backtest runs
-- ==========================================================================
create table if not exists backtest_trades (
    id uuid primary key default gen_random_uuid(),
    backtest_run_id uuid references backtest_runs(id) on delete cascade,
    created_at timestamptz not null default now(),

    ticker text not null,
    date text not null,
    direction text not null,
    conviction text not null,
    composite_score real not null,
    entry_price real not null,
    exit_price real not null,
    return_pct real not null,
    win boolean not null,
    regime text,
    layer_signals jsonb default '{}'::jsonb,
    layer_scores jsonb default '{}'::jsonb
);

create index if not exists idx_bt_trades_run on backtest_trades (backtest_run_id);
create index if not exists idx_bt_trades_ticker on backtest_trades (ticker);
create index if not exists idx_bt_trades_date on backtest_trades (date);

-- ==========================================================================
-- RLS: service_role bypasses RLS automatically.
-- Anon key gets read-only access (for future web UI).
-- ==========================================================================
alter table signals enable row level security;
alter table backtest_runs enable row level security;
alter table backtest_trades enable row level security;

create policy "Anon read signals"
    on signals for select
    using (true);

create policy "Anon read backtest_runs"
    on backtest_runs for select
    using (true);

create policy "Anon read backtest_trades"
    on backtest_trades for select
    using (true);
