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

-- List of replication groups present in the cluster, *including* us.
-- myself's srvid is null
create table repgroups (
	id int primary key,
        srvid oid  -- references pg_foreign_server(oid)
);

-- sharded tables
create table sharded_tables (
	rel oid primary key,  -- references pg_class(oid)
	nparts int,
	colocated_with oid references sharded_tables(rel)
);

-- main partitions
create table parts (
        rel oid references sharded_tables(rel),
	pnum int,
	part_name text,
	rgid int references repgroups(id), -- current owner
	primary key (rel, pnum)
);

/* ex wrapper */
create function ex_sql(rgid int, cmd text) returns void as 'MODULE_PATHNAME' language C;
/* Bcst wrapper */
create function bcst_sql(cmd text) returns void as 'MODULE_PATHNAME' language C;
/* BcstAll wrapper */
create function bcst_all_sql(cmd text) returns void as 'MODULE_PATHNAME' language C;

create function hash_shard_table(relid regclass, nparts int, colocate_with regclass = null) returns void as $$
declare
  rgids int[];
  nrgids int;
  rgid int;
  holder_rgid int;
  -- raw relname without any quotation
  relname name := relname from pg_class where oid = relid;
  part_name name;
  fdw_part_name name;
begin
  if exists (select 1 from hodgepodge.sharded_tables t where t.rel = relid) then
    raise exception 'table % is already sharded', relid;
  end if;
  if colocate_with is not null and not exists(select 1 from hodgepodge.sharded_tables t where t.rel = colocate_with) then
    raise exception 'colocated table % is not sharded', relid;
  end if;

  -- record table as sharded everywhere
  -- note that printing regclass automatically quotes it
  perform hodgepodge.bcst_all_sql(format('insert into hodgepodge.sharded_tables values (%L::regclass, %s, %L::regclass)',
                                         relid, nparts, colocate_with));

  -- Get repgroups in random order
  select array(select id from hodgepodge.repgroups order by random()) into rgids;
  nrgids := array_length(rgids, 1);

  for i in 0..nparts-1 loop
    if colocate_with is null then
      holder_rgid := rgids[1 + (i % nrgids)]; -- round robin
    else
      holder_rgid := p.rgid from hodgepodge.parts p where p.rel = colocate_with and p.pnum = i;
    end if;

    -- on each rg, create real or foreign partition
    part_name := format('%s_%s', relname, i);
    fdw_part_name := format('%s_fdw', part_name);
    raise log 'putting part % on %', part_name, holder_rgid;
    for rgid in select id from hodgepodge.repgroups loop
      if rgid = holder_rgid then
        perform hodgepodge.ex_sql(rgid, format('create table %I partition of %I for values with (modulus %s, remainder %s)',
	                                   part_name, relname, nparts, i));
      else
        perform hodgepodge.ex_sql(rgid, format('create foreign table %I partition of %I for values with (modulus %s, remainder %s) server hp_rg_%s options (table_name %L)',
	                                   fdw_part_name, relname, nparts, i, holder_rgid, quote_ident(part_name)));
      end if;
    end loop;
  end loop;
end $$ language plpgsql;

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


-- Type to represent vertex in lock graph
create type process as (node_sysid bigint, pid int);

-- View to build lock graph which can be used to detect global deadlock.
-- Application_name is assumed pgfdw:$system_id:$coord_pid
-- gid is assumed pgfdw:$timestamp:$sys_id:$pid:$xid:$participants_count:$coord_count
-- Currently we are oblivious about lock modes and report any wait -> hold edge
-- on the same object and therefore might produce false loops. Furthermore,
-- we have not idea about locking queues here. Probably it is better to use
-- pg_blocking_pids, but it seems to ignore prepared xacts.
create view lock_graph(wait, hold) as
        -- local dependencies
        -- if xact is already prepared, we take node and pid of the coordinator.
        select
                row((pg_control_system()).system_identifier, wait.pid)::hodgepodge.process,
                case when hold.pid is not null then
                    row((pg_control_system()).system_identifier, hold.pid)::hodgepodge.process
                else -- prepared
                    row(split_part(gid, ':', 3)::bigint, split_part(gid, ':', 4)::int)::hodgepodge.process
                end
        from pg_locks wait, pg_locks hold left outer join pg_prepared_xacts twopc
                on twopc.transaction=hold.transactionid
        where
                not wait.granted and wait.pid is not null and hold.granted and
                -- waiter waits for the the object holder locks
                wait.database is not distinct from hold.database and
                wait.relation is not distinct from hold.relation and
                wait.page is not distinct from hold.page and
                wait.tuple is not distinct from hold.tuple and
                wait.virtualxid is not distinct from hold.virtualxid and
                wait.transactionid is not distinct from hold.transactionid and -- waiting on xid
                wait.classid is not distinct from hold.classid and
                wait.objid is not distinct from hold.objid and
                wait.objsubid is not distinct from hold.objsubid and
                 -- this is most probably truism, but who knows
                (hold.pid is not null or twopc.gid is not null)
        union all -- only for performance; there is no uniques
        -- if this fdw backend is busy, potentially waiting, add edge coordinator -> fdw
        select row(split_part(application_name, ':', 2)::bigint,
                   split_part(application_name,':', 3)::int)::hodgepodge.process,
               row((pg_control_system()).system_identifier, pid)::hodgepodge.process
        from pg_stat_activity where application_name like 'pgfdw:%' and wait_event<>'ClientRead'
        union all -- only for performance; there is no uniques
        -- otherwise, coordinator itself is busy, potentially waiting, so add fdw ->
        -- coordinator edge
        select row((pg_control_system()).system_identifier, pid)::hodgepodge.process,
               row(split_part(application_name,':',2)::bigint, split_part(application_name,':',3)::int)::hodgepodge.process
        from pg_stat_activity where application_name like 'pgfdw:%' and wait_event='ClientRead';


-- to avoid bothering with custom types
create view lock_graph_native_types(wait_sysid, wait_pid, hold_sysid, hold_pid) as
    select (wait).node_sysid, (wait).pid, (hold).node_sysid, (hold).pid from lock_graph;

-- postgres_fdw
CREATE FUNCTION postgres_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION postgres_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER hodgepodge_postgres_fdw
  HANDLER postgres_fdw_handler
  VALIDATOR postgres_fdw_validator;
