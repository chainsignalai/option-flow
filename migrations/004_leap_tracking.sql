-- Track individual LEAP flow prints (180+ DTE) for accumulation detection
create table if not exists leap_flow (
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
    created_at timestamptz default now()
);

create index if not exists idx_leap_flow_ticker_date on leap_flow(ticker, created_at);
