-- Track when flow direction contradicts the composite signal direction.
-- After 60 days of forward data, analyze win rates for contradicted vs aligned trades.

alter table signals add column if not exists flow_contradicts boolean default false;

create index if not exists idx_signals_flow_contradicts on signals (flow_contradicts)
    where flow_contradicts = true;
