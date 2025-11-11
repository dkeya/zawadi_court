-- idempotent: safe to run many times
create schema if not exists public;

-- ---------- CORE LOOKUPS ----------
create table if not exists rates (
  rate_category text primary key,
  amount numeric(14,2) not null default 0
);

-- ---------- HOUSEHOLD CONTRIBUTIONS ----------
create table if not exists contributions (
  id bigserial primary key,
  house_no text unique,           -- natural key
  family_name text,
  lane text,
  rate_category text references rates(rate_category) on update cascade on delete set null,
  email text,
  cumulative_debt numeric(14,2) default 0,
  jan numeric(14,2) default 0, feb numeric(14,2) default 0, mar numeric(14,2) default 0,
  apr numeric(14,2) default 0, may numeric(14,2) default 0, jun numeric(14,2) default 0,
  jul numeric(14,2) default 0, aug numeric(14,2) default 0, sep numeric(14,2) default 0,
  oct numeric(14,2) default 0, nov numeric(14,2) default 0, dec numeric(14,2) default 0,
  ytd numeric(14,2) default 0,
  current_debt numeric(14,2) default 0,
  remarks text,
  updated_at timestamptz not null default now()
);
create index if not exists ix_contributions_lane on contributions(lane);
create index if not exists ix_contributions_rate on contributions(rate_category);

-- ---------- EXPENSES ----------
create table if not exists expenses (
  id bigserial primary key,
  "date" date,
  description text,
  category text,
  vendor text,
  phone text,
  amount_kes numeric(14,2) default 0,
  mode text,
  remarks text,
  receipt text,
  created_at timestamptz not null default now()
);
create index if not exists ix_expenses_date on expenses("date");
create index if not exists ix_expenses_category on expenses(category);

-- ---------- SPECIAL CONTRIBUTIONS ----------
create table if not exists special (
  id bigserial primary key,
  event text,
  "date" date,
  "type" text,
  contributors text,
  amount numeric(14,2) default 0,
  remarks text,
  created_at timestamptz not null default now()
);
create index if not exists ix_special_date on special("date");
create index if not exists ix_special_type on special("type");

-- ---------- REQUESTS ----------
create table if not exists expense_requests (
  id bigserial primary key,
  "date" date,
  description text,
  category text,
  requested_by text,
  amount_kes numeric(14,2) default 0,
  status text,
  remarks text,
  created_at timestamptz not null default now()
);

create table if not exists contribution_requests (
  id bigserial primary key,
  "date" date,
  month text,
  family_name text,
  house_no text,
  lane text,
  rate_category text,
  amount_kes numeric(14,2) default 0,
  status text,
  remarks text,
  created_at timestamptz not null default now()
);

create table if not exists special_requests (
  id bigserial primary key,
  "date" date,
  event text,
  "type" text,
  requested_by text,
  amount numeric(14,2) default 0,
  status text,
  remarks text,
  created_at timestamptz not null default now()
);

-- ---------- CASH MANAGEMENT (single row) ----------
create table if not exists cash_management (
  id smallint primary key default 1,
  cash_balance_cd numeric(14,2) default 0,
  cash_withdrawal numeric(14,2) default 0,
  updated_at timestamptz not null default now()
);
insert into cash_management (id) values (1)
on conflict (id) do nothing;
