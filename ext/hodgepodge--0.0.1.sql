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
