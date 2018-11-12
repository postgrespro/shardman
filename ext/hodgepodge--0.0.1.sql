/* ------------------------------------------------------------------------
 *
 * hodgepodge.sql
 *
 * Copyright (c) 2018, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION hodgepodge" to load this file. \quit

-- List of nodes present in the cluster
create table nodes (
	id int primary key,
	system_id bigint not null unique,
	connection_string text unique not null
);

-- list of sharded tables
create table tables (
	relation text primary key     -- table name
);

-- main partitions
create table partitions (
	part_name text primary key,
	node_id int references nodes(id),
	relation text not null references tables(relation) on delete cascade
);

-- Get subscription status
create function is_subscription_ready(sname text) returns bool as $$
declare
	n_not_ready bigint;
begin
	select count(*) into n_not_ready from pg_subscription_rel srel
		join pg_subscription s on srel.srsubid = s.oid where subname=sname and srsubstate<>'r';
	return n_not_ready=0;
end
$$ language plpgsql;

-- brusquely and reliably (with persistence) forbid writes to the table
create function write_protection_on(part regclass) returns void as $$
begin
	if not exists (select 1 from pg_trigger where tgname = 'write_protection' and tgrelid = part) then
		execute format('create trigger write_protection before insert or update or delete or truncate on
					   %I for each statement execute procedure hodgepodge.deny_access();',
					   part::name);
	end if;
end
$$ language plpgsql;

create function write_protection_off(part regclass) returns void as $$
begin
	if exists (select 1 from pg_trigger where tgname = 'write_protection' and tgrelid = part) then
		execute format('drop trigger write_protection on %I', part::name);
	end if;
end
$$ language plpgsql;

create function deny_access() returns trigger as $$
begin
    raise exception 'this partition was moved to another node';
end
$$ language plpgsql;
