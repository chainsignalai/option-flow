create table if not exists paper_trade_events (
    id uuid default gen_random_uuid() primary key,
    position_id text not null,
    ticker text not null,
    event_type text not null,
    direction text,
    option_type text,
    strike numeric,
    expiry date,
    price numeric,
    filled_price numeric,
    pnl_pct numeric,
    peak_premium numeric,
    trail_active boolean default false,
    close_reason text,
    metadata jsonb default '{}'::jsonb,
    created_at timestamptz default now()
);

create index idx_paper_events_position on paper_trade_events (position_id);
create index idx_paper_events_type on paper_trade_events (event_type);
create index idx_paper_events_ticker on paper_trade_events (ticker);
create index idx_paper_events_created on paper_trade_events (created_at);
