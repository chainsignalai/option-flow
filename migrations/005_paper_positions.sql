-- Paper positions stored in Supabase instead of local JSON file.
-- Survives DO restarts, prevents duplicate trades.
create table if not exists paper_positions (
    order_id text primary key,
    ticker text not null,
    direction text not null,
    option_type text not null,
    strike numeric not null,
    expiry text not null,
    quantity integer not null default 1,
    limit_price numeric not null,
    occ_symbol text not null,
    status text not null default 'PENDING',
    filled_price numeric,
    filled_at text,
    premium_target_pct numeric default 50.0,
    premium_stop_pct numeric default -40.0,
    trail_activate_pct numeric default 30.0,
    trail_stop_pct numeric default 20.0,
    max_hold_days integer default 10,
    theta_kill_days integer default 5,
    theta_kill_move_pct numeric default 10.0,
    underlying_entry numeric default 0.0,
    peak_premium numeric,
    trail_active boolean default false,
    opened_at text not null,
    closed_at text,
    close_reason text default '',
    close_price numeric,
    pnl_pct numeric,
    strategy_type text default 'SWING',
    created_at timestamptz default now(),
    updated_at timestamptz default now()
);

create index if not exists idx_paper_positions_ticker on paper_positions(ticker);
create index if not exists idx_paper_positions_status on paper_positions(status);
create index if not exists idx_paper_positions_strategy on paper_positions(strategy_type);
