--
-- BEGIN_COPYRIGHT
--
-- Copyright (C) 2008-2018 SciDB, Inc.
-- All Rights Reserved.
--
-- This file is part of the Paradigm4 Enterprise SciDB distribution kit
-- and may only be used with a valid Paradigm4 contract and in accord
-- with the terms and conditions specified by that contract.
--
-- END_COPYRIGHT
--
-- CLEAR
--
-- ---------------------------------------------------------------------
-- DROP TABLES
-- ---------------------------------------------------------------------
drop table if exists "array" cascade;
drop table if exists "array_distribution" cascade;
drop table if exists "array_residency" cascade;
drop table if exists "array_version" cascade;
drop table if exists "latest_array_version" cascade;
drop table if exists "array_version_lock" cascade;
drop table if exists "instance" cascade;
drop table if exists "array_attribute" cascade;
drop table if exists "array_dimension" cascade;
drop table if exists "cluster" cascade;
drop table if exists "libraries" cascade;
drop table if exists "users" cascade;
drop table if exists "namespaces" cascade;
drop table if exists "namespace_members" cascade;
drop table if exists "provisional_arrays" cascade;
drop table if exists "roles" cascade;
drop table if exists "role_members" cascade;
drop table if exists "role_namespace_permissions" cascade;

-- ---------------------------------------------------------------------
-- DROP VIEWS
-- ---------------------------------------------------------------------
drop view if exists namespace_arrays cascade;
drop view if exists public_arrays cascade;
drop view if exists namespace_members_view cascade;

-- ---------------------------------------------------------------------
-- DROP SEQUENCES
-- ---------------------------------------------------------------------
drop sequence if exists "array_id_seq" cascade;
drop sequence if exists "array_distribution_id_seq" cascade;
drop sequence if exists "instance_id_seq" cascade;
drop sequence if exists "libraries_id_seq" cascade;
drop sequence if exists "user_id_seq" cascade;
drop sequence if exists "namespaces_id_seq" cascade;
drop sequence if exists "role_id_seq" cascade;
drop sequence if exists "role_permissions_id_seq" cascade;
drop sequence if exists "role_namespace_permissions_id_seq" cascade;

-- ---------------------------------------------------------------------
-- DROP FUNCTIONS
-- ---------------------------------------------------------------------
drop function if exists uuid_generate_v1();
drop function if exists get_cluster_uuid();
drop function if exists get_metadata_version();
drop function if exists check_uaid_in_array(bigint);
drop function if exists check_server_id_host(integer,varchar);
drop function if exists check_base_path(varchar);


-- ---------------------------------------------------------------------
-- CREATE SEQUENCES
-- ---------------------------------------------------------------------
create sequence "array_id_seq";
create sequence "array_distribution_id_seq" minvalue 0 start with 0;
create sequence "instance_id_seq" minvalue 0 start with 0;
create sequence "libraries_id_seq";
create sequence "user_id_seq" minvalue 1 start with 2;
create sequence "namespaces_id_seq" minvalue 1 start with 2;
create sequence "role_id_seq" minvalue 1 start with 2;
create sequence "role_permissions_id_seq" minvalue 1 start with 1;
create sequence "role_namespace_permissions_id_seq";

-- ---------------------------------------------------------------------
-- CREATE TABLES
-- ---------------------------------------------------------------------
create table "cluster"
(
  cluster_uuid uuid,
  metadata_version integer
);


--
--  Table: array_distribution
--
-- Description of array distribution for each UNVERSIONED array
--
-- array_distribution.id : unique ID for this distribution
--
-- array_distribution.partition_function : array partitioning scheme
-- (for now) one of {psHashPartitioned=0, psLocalInstance=1, psByRow=2, psByCol=3, ...}
--
-- array_distribution.partition_state : any additional state required to define the distribution
-- (e.g. processor grid)
--
-- array_distribution.redundancy : number of (additional) data replicas
-- (the total number of copies == redundancy+1)

create table "array_distribution"
(
  id bigint primary key default nextval('array_distribution_id_seq'),
  partition_function integer,
  partition_state varchar,
  redundancy integer
);


--
--  Table: "array"  (public.array) List of arrays in the SciDB installation.
--
--          Information about all persistent arrays in the SciDB installation
--          are recorded in public.array.
--
--   SciDB arrays recorded in public.array come in several forms.
--
--   1. Basic (or persistent) Arrays: Arrays named in CREATE ARRAY statements.
--
--   2. Array Versions: SciDB supports data modification by creating new
--      version of basic arrays. Each new version of a Basic array gets its
--      own entry in public.array.
--
--   SciDB creates rows in the public.array catalog to record the existance
--  of many things; arrays created by users, each version of these arrays,
--  and arrays created to hold non-integer dimension values and their
--  mappings to array dimension offsets.
--
--   public.array.id - unique int64 identifier for the array. This synthetic
--            key is used to maintain references within the catalogs, and to
--            identify the array within the SciDB engine's own metadata:
--            ArrayDesc._id.
--
-- public.array.name - the array's name. If the array is a Basic or persistent
--            array the public.array.name reflects the name as it appears in
--            the CREATE ARRAY statement. If the entry corresponds to a
--            version of an array, then the contents of this field consist of
--            the array name plus the version number.
--
--public.array.partitiong_scheme - SciDB supports several partitioning schemes.
--
--                     0. Replication of the array's contents on all instances.
--                     1. Round Robin allocation of chunks to instances.
--
-- public.array.flags - records details about the array's status.
--
--


--
--  Table: "array"  (array) List of arrays in the SciDB installation.
--
--          Information about all persistent arrays in the SciDB installation
--          are recorded in array.
--
--   SciDB arrays recorded in array come in several forms.
--
--   1. Unversioned Arrays: Arrays named in CREATE ARRAY statements.
--
--   2. Array Versions: SciDB supports data modification by creating new
--      version of unversioned arrays. Each new version of a Unversioned array gets its
--      own entry in array.
--
--   SciDB creates rows in the array catalog to record the existance
--  of many things; arrays created by users, each version of these arrays.
--
-- array.id : unique int64 identifier for the array. This synthetic
--            key is used to maintain references within the catalogs, and to
--            identify the array within the SciDB engine's own metadata:
--            ArrayDesc._id.
--
-- array.name : the array's name. If the array is a Unversioned or persistent
--            array the array.name reflects the name as it appears in
--            the CREATE ARRAY statement. If the entry corresponds to a
--            version of an array, then the contents of this field consist of
--            the array name plus the version number.
--
-- array.flags : records details about the array's status.
--
-- array.distribution_id : a reference to an array distribution from the "array_distribution" table.
--
--
create table "array"
(
  id bigint primary key default nextval('array_id_seq'),
  name varchar,
  flags integer,
  distribution_id bigint references "array_distribution" (id),

  unique ( id, name ));


--
--   Table: public.array_version
--
--      Information about array versions, their relationship to the Basic
--   arrays and the entries in the public.array table, and their creation
--   timestamps.
--
--  public.array_version.array_id - reference back to the entry in the
--                                  public.array.id column that corresponds
--                                  to the Basic array.
--
--  public.array_version.version_id - the (sequential) number of the version.
--
--  public.array_version.version_array_id - reference back to the entry in the
--                  public.array.id column that identifies the Versioned Array.
--
--  public.array_version.time_stamp - timestamp (in seconds since epoch) at
--                  which the version was created.
--
--  PGB: I worry that time-to-the-second might not be sufficient precision.
--
create table "array_version"
(
   array_id bigint references "array" (id) on delete cascade,
   version_id bigint,
   version_array_id bigint references "array" (id) on delete cascade,
   time_stamp bigint,
   primary key(array_id, time_stamp, version_id),
   unique(array_id, version_id)
);

--
--   Table: public.latest_array_version
--
--      Holds the array_id of the latest version of any given array.  Note
--   that this is not the array version, but the latest array id for any
--   given version of an array.  This table effectively serves as a cache
--   to avoid a full scan over the "array" table to determine the same
--   information via an aggregate.
--
--  public.latest_array_version.namespace_name - The name of the namespace in
--                                               which the target array resides.
--
--  public.latest_array_version.array_name - The name of the target array
--
--  public.latest_array_version.array_id - The latest array id for the given array.
--
-- 'read' queries of this table have the namespace and array names
--
create table latest_array_version
(
   namespace_name varchar,
   array_name varchar,
   array_id bigint references "array" (id) on delete cascade,
   primary key(namespace_name,array_name)
);

--
--  Table: public.instance
--
--    Information about the SciDB instances that are part of this installation.
--
create table "instance"
(
  instance_id bigint primary key,
  membership_id bigint default 0,
  host varchar,
  port integer,
  online_since timestamp,
  base_path varchar,
  server_id integer,
  server_instance_id integer,
  unique(host,port),
  unique(server_id,server_instance_id),
  unique(host,server_id,server_instance_id)
);

-- Ensure that all server_id=X records use the same host (that is,
-- there are none that use a different host).  This check wouldn't be
-- needed if we had a normalized database, oh well.
CREATE FUNCTION check_server_id_host(integer, varchar)
RETURNS BOOLEAN AS
$$
SELECT COALESCE ((SELECT false FROM public."instance"
                  WHERE server_id = $1 AND host <> $2
                  LIMIT 1),  -- found violation
                  true)      -- no violation
$$ LANGUAGE SQL;

alter table "instance"
add constraint instance_server_id_host_unique CHECK (check_server_id_host(server_id,host));

-- All base_path fields must be the same (for now anyway).  Not very normalized, oh well.
CREATE FUNCTION check_base_path(varchar)
RETURNS BOOLEAN AS
$$
SELECT COALESCE ((SELECT false FROM public."instance"
                  WHERE base_path <> $1
                  LIMIT 1),  -- found violation
                  true)      -- no violation
$$ LANGUAGE SQL;

alter table "instance"
add constraint instance_base_path_non_unique CHECK (check_base_path(base_path));


--
--  Table: public.array_version_lock
--
--    Information about the locks held by currently running queries.
--
create table "array_version_lock"
(
   namespace_name varchar,
   array_name varchar,
   array_id bigint,
   query_id bigint,
   instance_id bigint references "instance" (instance_id),
   array_version_id bigint,
   array_version bigint,
   coordinator_id bigint references "instance" (instance_id),
   lock_mode integer, -- {0=invalid, read, write, remove, renameto, renamefrom}
   unique(namespace_name, array_name, coordinator_id, query_id, instance_id)
);

--
--  Table: public.provisional_arrays
--
--  Provisional array metadata as generated during a grouped query are stored
--  here for rollback until the grouped query commits or aborts.
--
--  public.array_id - The unversioned array ID.
--  public.array_version_id - The versioned array ID.
--  public.array_version - The version (X in a@X) for this array.
--  public.instance_id - The instance ID creating this record.
--  public.multiquery_id - The query ID of the multiquery responsible for the
--          subquery that created this provisional array entry.
--
create table "provisional_arrays"
(
   array_id bigint,
   array_version_id bigint,
   array_version bigint,
   instance_id bigint references "instance" (instance_id),
   multiquery_id varchar,
   unique(array_id, array_version_id, array_version, instance_id)
);

--
--  Table: public.membership
--
--    Single row table to maintain a monotonically increasing membership ID
--
drop table if exists "membership" cascade;
create table "membership"
(
  id bigint primary key default 0
);
insert into "membership" (id) values (0);

--
--  Table: public.array_attribute
--
--     Information about each array's attributes.
--
--  public.array_attribute.array_id - reference to the entry in the
--          public.array catalog containing details about the array to which
--          this attribute belongs.
--
--          Each new array version creates an entirely new set of entries in
--          the public.array_attribute and the public.array_dimension catalogs.
--
--  public.array_attribute.id - defines the order of the attribute within the
--                              array.
--
--  public.array_attribute.name - name of the attribute as it appears in the
--           CREATE ARRAY ... statement.
--
--  public.array_attribute.type - data type of the attribute. Note that the
--           types are not recorded in the catalogs, but instead are named
--           when the types are loaded into each instance. The idea is that
--           using integer identifiers would make it hard to disentangle
--           changes in type implementation.
--
--  public.array_attribute.flags - information about the attribute.
--
--  public.array_attribute.default_compression_method - compression method
--            used on the attribute's chunks.
--
--  public.array_attribute.reserve - SciDB organizes attribute data into
--            chunks, which are the physical unit of storage and
--            inter-instance communication. To support modifications to the
--            array each chunk reserves a block of memory to hold backwards
--            deltas that can be applied to the chunk's contents to recover
--            previous versions. The value in this column reflects the
--            percentage of the chunk which is set aside (reserved) to hold
--            deltas.
--
-- public.array_attribute.default_missing_reason - if the attribute has a
--            DEFAULT with a missing code, the missing code is recorded here.
--            If the attribute has no default missing code (and by default,
--            SciDB array attributes are prevented from having missing codes).
--
-- public.array_attribute.default_value - if the attribute has a DEFAULT value,
--            the default value is recorded here. When a DEFAULT value is
--            calculated from an expression, the expression is placed here.
--
create table "array_attribute"
(
  array_id bigint references "array" (id) on delete cascade,
  id int,
  name varchar not null,
  type varchar,
  flags int,
  default_compression_method int,
  reserve int,
  default_missing_reason int not null,
  default_value varchar null,
  primary key(array_id, id),
  unique ( array_id, name )
);
--
-- Table: public.array_dimension
--
--    Information about array dimensions.
--
--  Array dimensions come in three forms.
--
--  1. There are integer (whole number) dimensions that correspond to the
--     simplest and most basic arrays.
--
--  2. There are non-integer dimensions where the values that make up the
--     collection of labels on the dimension are organized into an array.
--
--  3. There are non-integer dimensions where a pair of functions are used
--     to map from the dimension's type to int64 values, and vice-versa.
--
--  public.array_dimension.array_id - reference to the entry in the
--                          public.array catalog containing details about
--                          the array to which this dimension belongs.
--
--                           Each new array version creates an entirely new
--                          set of entries in the public.array_attribute and
--                          the public.array_dimension catalogs.
--
--  public.array_dimension.id - order of the dimension within the array's
--                              shape. The combination of the array id and
--                              this id make up the key of this catalog.
--
--  public.array_dimension.name - the name of the array dimension.
--
--  public.array_dimension.startMin
--                        .currStart
--                        .currEnd
--                        .endMax
--
--     Regardless of the data types used in the dimension, all array
--   dimensions have an extent defined in terms of the integer offsets into
--   the dimension space. Generally, array dimensions have an initial offset
--   of zero (0), but then some maximum length (say 100). Now, it is also true
--   that when they are declared, array dimensions can be given proscribed
--   limits. For example, we can declare that an array can only be 200 long
--   on some dimension. Also, arrays can be unbounded in their declaration,
--   but in practice can currently have only a current length.
--
--    These four columns of the public.array_dimension catalog record the
--   minimum legal start value, the current start value, the current end value
--   and the maximum end value for the dimension. The dimension's length is
--   currEnd - currStart.
--
--  public.array_dimension.chunk_interval - length of the chunks in this
--               dimension.
--
--  public.array_dimension.chunk_overlap  - overlap of the chunks in this
--               dimension.
--
--  public.array_dimension.type - name of the data type for the dimension, if
--               the dimension is declared as a non-integer type.
--
--
create table "array_dimension"
(
  array_id bigint references "array" (id) on delete cascade,
  id int,
  name varchar not null,
  startMin bigint,
  currStart bigint,
  currEnd bigint,
  endMax bigint,
--
  chunk_interval bigint,
  chunk_overlap bigint,
  primary key(array_id, id),
  unique ( array_id, name )
);

create table "libraries"
(
  id bigint primary key default nextval('libraries_id_seq'),
  name varchar unique
);


--
--  Table: public.users
--
--  Maps a SciDB user to their id and password.
--
--  public.users.id - The (sequential) number of the user
--
--  public.users.name - The user's name
--
--  public.users.password - The user's password
--
--
create table "users"
(
  id bigint primary key default nextval('user_id_seq'),
  name varchar unique,
  method varchar,
  password varchar,
  salt varchar
);

--
--  Table: public.namespaces
--
--  Maps a SciDB namespace to its id.
--
--  public.namespaces.id - The (sequential) number of the
--      namespace
--
--  public.namespaces.name - The name of the namespace
--
--
create table "namespaces"
(
  id bigint primary key default nextval('namespaces_id_seq'),
  name varchar unique
);

--
--  Table: public.namespace_members
--
--  Maps a SciDB namespace to its corresponding arrays
--
--  public.namespaces.namespace_id - The (sequential) number of the
--      namespace
--
--  public.namespaces.array_id - The array.id in the namespace
--
--
create table "namespace_members"
(
  namespace_id bigint references "namespaces" (id) on delete cascade,
  array_id bigint references "array" (id) on delete cascade,

  primary key(namespace_id, array_id)
);

--
--  Table: public.roles
--
--  Maps a SciDB role to its id.
--
--  public.roles.id - The (sequential) number of the role
--
--  public.roles.name - The name of the role
--
--
create table "roles"
(
  id bigint primary key default nextval('role_id_seq'),
  name varchar unique
);

--
--  Table: public.role_members
--
--  Represents which users belong to which roles
--
--  public.role_members.role_id - The id that represents the role
--
--  public.role_members.user_id - The id that represents the
--    user that belongs to the role specified by role_id.
--
--
create table "role_members"
(
  role_id bigint references "roles" (id) on delete cascade,
  user_id bigint references "users" (id) on delete cascade,

  primary key(role_id, user_id)
);


--
--  Table: public.role_namespace_permissions
--
--  Maps a SciDB role and a namespace to the corresponding permissions
--
--  public.role_namespace_permissions.role_id - The id of the role
--
--  public.role_namespace_permissions.namespace_id - The id of the namespace
--
--  public.role_namespace_permissions.permissions - The permissions for which the role has on the namespace
--         c=create array within namespace
--         r=read array within namespace
--         u=update (write) array within namespace
--         d=delete array within namespace
--         l=list arrays within namespace
--         a=administer namespace
--
--     Example:  Providing all permissions for John(id=2) on namespace NS1(id=5):
--         role_id=2, namespace_id=5, permissions="crudla"
--
--     Example:  Providing only read and list permissions for Jenny(id=4) on namespace NS2(id=7):
--         role_id=4, namespace_id=7, permissions="rl"
--
--
create table "role_namespace_permissions"
(
  id bigint primary key default nextval('role_namespace_permissions_id_seq'),
  role_id bigint references "roles" (id) on delete cascade,
  namespace_id bigint references "namespaces" (id) on delete cascade,
  permissions varchar,

  unique(role_id, namespace_id)
);


create or replace function uuid_generate_v1()
returns uuid
as '$libdir/uuid-ossp', 'uuid_generate_v1'
volatile strict language C;


-- The version number corresponds to the var METADATA_VERSION from Constants.h
-- If we start and find that cluster.metadata_version is less than METADATA_VERSION
-- upgrade. The upgrade files are provided as sql scripts in
-- src/system/catalog/data/[NUMBER].sql. They are converted to string
-- constants in a C++ header file  (variable METADATA_UPGRADES_LIST[])
-- and then linked in at build time.
-- @see SystemCatalog::connect(const string&, bool)
-- Note: there is no downgrade path at the moment.
insert into "cluster" values (uuid_generate_v1(), 5);

insert into users (id, name, password, method) values (
    1,
    'scidbadmin',
    E'eUCUk3B57IVO9ZfJB6CIEHl/0lxrWg/7PV8KytUNY6kPLhTX2db48GHGHoizKyH+uGkCfNTYZrJgKzjWOhjuvg==',
    'raw');
insert into namespaces (name, id) values ('public', 1);
insert into roles (id, name) values (1, 'scidbadmin');
insert into role_members (role_id, user_id) values(1,1);

create function get_cluster_uuid() returns uuid as $$
declare t uuid;
begin
  select into t cluster_uuid from public."cluster" limit 1;
  return t;
end;
$$ language plpgsql;

create function get_metadata_version() returns integer as $$
declare v integer;
begin
  select into v metadata_version from public."cluster" limit 1;
  return v;
end;
$$ language plpgsql;

-- Ensure that unversioned array id is present in array table.  Not
-- done using a foreign key constraint because of the "NOT LIKE '%@%'"
-- clause.
CREATE FUNCTION check_uaid_in_array(bigint)
RETURNS BOOLEAN AS
$$
SELECT COALESCE ((SELECT true FROM public."array"
                  WHERE id = $1 AND name NOT LIKE '%@%'
                  LIMIT 1),  -- uaid is valid
                  false)     -- no such uaid
$$ LANGUAGE SQL;


--
--  Table: array_residency
--
-- Description of array residency for each UNVERSIONED array
--
-- array_residency.array_id : unversioned array ID
-- array_residency.instance_id : instance ID

create table "array_residency"
(
  array_id bigint CHECK (check_uaid_in_array(array_id)),
  instance_id bigint references "instance" (instance_id),
  primary key(array_id, instance_id)
);

-- ---------------------------------------------------------------------
-- VIEWS
-- ---------------------------------------------------------------------
CREATE VIEW namespace_members_view AS (
    SELECT namespace_id, array_id FROM namespace_members
    UNION
        SELECT nm.namespace_id, av.version_array_id as array_id
        FROM  array_version as av, namespace_members as nm
        WHERE av.array_id=nm.array_id
);

CREATE VIEW namespace_arrays AS (
    SELECT
		CASE WHEN NS.name IS NULL THEN 'public' ELSE NS.name end AS namespace_name,
		ARR.name AS array_name,
		CASE when MEM.namespace_id IS null THEN 1 ELSE  MEM.namespace_id END AS namespace_id,
		ARR.id AS array_id,
        ARR.flags AS flags,
        ARR.distribution_id as distribution_id
	FROM "array" as ARR
    LEFT JOIN
        namespace_members_view as MEM
    ON
        MEM.array_id = ARR.id
    LEFT JOIN
        namespaces as NS
    ON
        NS.id=MEM.namespace_id
);

CREATE VIEW public_arrays AS (
    SELECT  NSA.array_name AS name,
            NSA.array_id AS id,
            NSA.flags,
            NSA.distribution_id
    FROM namespace_arrays AS NSA
    WHERE NSA.namespace_id=1
);
