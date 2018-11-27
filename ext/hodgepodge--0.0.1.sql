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
	colocated_with oid references sharded_tables(rel),
	-- a bit weird to store, but these are useful for dump-restore to new node
        relname name,
	nspname name
);

-- main partitions
create table parts (
        rel oid references sharded_tables(rel) on update cascade,
	pnum int,
	rgid int references repgroups(id), -- current owner
	primary key (rel, pnum)
);

-- fill relname and nspname on insertion to sharded_tables automatically
create function sharded_tables_fill_relname() returns trigger as $$
begin
  if new.relname is null then -- don't during restore
    new.relname := relname from pg_class where oid = new.rel;
    new.nspname := nspname from pg_namespace n, pg_class c where c.oid = new.rel and n.oid = c.relnamespace;
  end if;
  return new;
end $$ language plpgsql;
create trigger sharded_tables_fill_relname before insert on hodgepodge.sharded_tables
  for each row execute function sharded_tables_fill_relname();

-- executed on new rg after dump/restore
create function hodgepodge.restamp_oids() returns void as $$
begin
  update hodgepodge.sharded_tables s set rel = format('%I.%I', s.nspname, s.relname)::regclass;
end $$ language plpgsql;

-- executed on new rg
create or replace function restore_foreign_tables() returns void as $$
declare
  relid oid;
  pnum int;
  holder_rgid int;
  relname name;
  part_name name;
  fdw_part_name name;
  nparts int;
  me int := id from hodgepodge.repgroups where srvid is null;
begin
  set local hodgepodge.broadcast_utility to off;
  for relid, nparts in select st.rel, st.nparts from hodgepodge.sharded_tables st loop
    relname := c.relname from pg_class c where oid = relid;
    for pnum, holder_rgid in select parts.pnum, rgid from hodgepodge.parts parts where parts.rel = relid loop
      assert holder_rgid != me, 'new rg holds partitions';
      part_name := format('%s_%s', relname, pnum);
      fdw_part_name := format('%s_fdw', part_name);
      execute format('drop table if exists %I', part_name);
      execute format('drop foreign table if exists %I', fdw_part_name);
      execute format('create foreign table %I partition of %I for values with (modulus %s, remainder %s) server hp_rg_%s options (table_name %L)',
	                                   fdw_part_name, relname, nparts, pnum, holder_rgid, quote_ident(part_name));
    end loop;
  end loop;
end $$ language plpgsql;

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
  holder_rgid int not null = -1;
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
      perform hodgepodge.ex_sql(rgid, format('insert into hodgepodge.parts values (%L::regclass, %s, %s)',
                                             quote_ident(relname), i, holder_rgid));
    end loop;
  end loop;
end $$ language plpgsql;

-- update foreign tables everywhere according to the part move
create function part_moved(relid regclass, pnum int, src_rgid int, dst_rgid int) returns void as $$
declare
  -- raw relname without any quotation
  relname name not null := relname from pg_class where oid = relid;
  part_name name := format('%s_%s', relname, pnum);
  fdw_part_name name := format('%s_fdw', part_name);
  nparts int not null := nparts from hodgepodge.sharded_tables where rel = relid;
  rgid int;
begin
  for rgid in select id from hodgepodge.repgroups loop
    if rgid = dst_rgid then
      /* drop foreign, attach real */
      perform hodgepodge.ex_sql(rgid, format('drop foreign table %I', fdw_part_name));
      perform hodgepodge.ex_sql(rgid, format('alter table %I attach partition %I for values with (modulus %s, remainder %s)',
                                             relname, part_name, nparts, pnum));
    elsif rgid = src_rgid then
      /* drop real, attach foreign */
      perform hodgepodge.ex_sql(rgid, format('drop table %I', part_name));
      perform hodgepodge.ex_sql(rgid, format('create foreign table %I partition of %I for values with (modulus %s, remainder %s) server hp_rg_%s options (table_name %L)', fdw_part_name, relname, nparts, pnum, dst_rgid, quote_ident(part_name)));
    else
      /* recreate foreign */
      perform hodgepodge.ex_sql(rgid, format('drop foreign table %I', fdw_part_name));
      perform hodgepodge.ex_sql(rgid, format('create foreign table %I partition of %I for values with (modulus %s, remainder %s) server hp_rg_%s options (table_name %L)', fdw_part_name, relname, nparts, pnum, dst_rgid, quote_ident(part_name)));
    end if;
    perform hodgepodge.ex_sql(rgid, format('update hodgepodge.parts set rgid = %s where rel = %L::regclass and pnum = %s',
                              dst_rgid, quote_ident(relname), pnum));

  end loop;
end $$ language plpgsql;

create function rmrepgroup(rmrgid int) returns void as $$
declare
  rgid int;
begin
  for rgid in select id from hodgepodge.repgroups loop
    if rgid = rmrgid then -- don't touch removed rg
      continue;
    end if;
    perform hodgepodge.ex_sql(rgid, format('delete from hodgepodge.repgroups where id = %s', rmrgid));
    perform hodgepodge.ex_sql(rgid, format('drop server if exists hp_rg_%s cascade', rmrgid));
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

-- rebalance stuff
create function rebalance_cleanup() returns void as $$
begin
  perform hodgepodge.bcst_all_sql('select hodgepodge.rebalance_cleanup_local()');
end $$ language plpgsql;

-- drop subs, slots, pubs used for rebalance and potential orphaned partitions
create function rebalance_cleanup_local() returns void as $$
declare
  sub record;
  rs record;
  pub record;
  relid oid;
  relname name;
  pnum int;
  holder_rgid int;
  part_name name;
  me int := id from hodgepodge.repgroups where srvid is null;
begin
  set local hodgepodge.broadcast_utility to off;

  for sub in select subname from pg_subscription where subname like 'hp_copy_%' loop
		perform hodgepodge.eliminate_sub(sub.subname);
  end loop;
  for rs in select slot_name from pg_replication_slots where slot_name like 'hp_copy_%' and slot_type = 'logical' loop
    perform hodgepodge.drop_repslot(rs.slot_name, true);
  end loop;
  raise warning 'going to drop pubies';
  for pub in select pubname from pg_publication where pubname like 'hp_copy_%' loop
    raise warning 'drooping pub %', pub.pubname;
    execute format('drop publication %I', pub.pubname);
  end loop;

  for relid in select st.rel, st.nparts from hodgepodge.sharded_tables st loop
    relname := c.relname from pg_class c where oid = relid;
    for pnum, holder_rgid in select parts.pnum, rgid from hodgepodge.parts parts where parts.rel = relid loop
      part_name := format('%s_%s', relname, pnum);
      if holder_rgid != me then
        execute format('drop table if exists %I', part_name);
      end if;
    end loop;
  end loop;
end $$ language plpgsql;

create function eliminate_sub(subname name) returns void as $$
declare
	sub_exists bool;
begin
  execute format('select exists (select 1 from pg_subscription where subname = %L)',
	         subname) into sub_exists;
  if sub_exists then
    execute format('alter subscription %I disable', subname);
    execute format('alter subscription %I set (slot_name = none)', subname);
    execute format('drop subscription %I', subname);
  end if;
end $$ language plpgsql strict;

-- Drop replication slot, if it exists.
-- About 'with_force' option: we can't just drop replication slots because
-- pg_drop_replication_slot will bail out with ERROR if connection is active.
-- Therefore the caller must either ensure that the connection is dead (e.g.
-- drop subscription on far end) or pass 'true' to 'with_force' option, which
-- does the following dirty hack. It kills several times active walsender with
-- short interval. After the first kill, replica will immediately try to
-- reconnect, so the connection resurrects instantly. However, if we kill it
-- second time, replica won't try to reconnect until wal_retrieve_retry_interval
-- after its first reaction passes, which is 5 secs by default. Of course, this
-- is not reliable and should be redesigned.
create function drop_repslot(slot_name text, with_force bool default true) returns void AS $$
declare
  slot_exists bool;
  kill_ws_times int := 3;
begin
  raise debug '[HP] Dropping repslot %', slot_name;
  execute format('select exists (select 1 from pg_replication_slots where slot_name = %L)',
                 slot_name) into slot_exists;
  IF slot_exists then
    IF with_force then -- kill walsender several times
      raise debug '[HP] Killing repslot % with fire', slot_name;
      for i in 1..kill_ws_times loop
        raise debug '[HP] Killing walsender for slot %', slot_name;
	perform hodgepodge.terminate_repslot_walsender(slot_name);
	if i != kill_ws_times then
	  perform pg_sleep(0.05);
	end if;
      end loop;
    end if;
    execute format('select pg_drop_replication_slot(%L)', slot_name);
  end if;
end $$ language plpgsql strict;
create function terminate_repslot_walsender(slot_name text) returns void as $$
begin
  execute format('select pg_terminate_backend(active_pid) from pg_replication_slots WHERE slot_name = %L', slot_name);
end
$$ language plpgsql strict;


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
