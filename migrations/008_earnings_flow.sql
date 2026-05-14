-- Pre-earnings flow accumulation tracking.
-- Stores individual flow prints on tickers with upcoming earnings for scanner.
create table if not exists earnings_flow (
    id uuid default gen_random_uuid() primary key,
    ticker text not null,
    option_type text not null,
    strike numeric not null,
    expiry date not null,
    dte integer not null,
    premium numeric not null,
    is_sweep boolean default false,
    side text,
    sentiment text,
    underlying_price numeric,
    vol_oi_ratio numeric,
    earnings_date date,
    created_at timestamptz default now()
);

create index if not exists idx_earnings_flow_ticker_date on earnings_flow(ticker, created_at);
create index if not exists idx_earnings_flow_earnings on earnings_flow(earnings_date);
