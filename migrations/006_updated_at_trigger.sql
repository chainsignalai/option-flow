-- Auto-update updated_at on paper_positions row modification
create or replace function update_updated_at()
returns trigger as $$
begin
    NEW.updated_at = now();
    return NEW;
end;
$$ language plpgsql;

create trigger paper_positions_updated_at
    before update on paper_positions
    for each row
    execute function update_updated_at();
