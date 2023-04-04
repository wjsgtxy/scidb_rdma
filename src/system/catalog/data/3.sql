--upgrade from 2 to 3


-- ---------------------------------------------------------------------
-- CREATE SEQUENCES
-- ---------------------------------------------------------------------
create sequence "user_id_seq";
create sequence "namespaces_id_seq" minvalue 1 start with 2;

-- ---------------------------------------------------------------------
-- CREATE TABLES
-- ---------------------------------------------------------------------

-- ---------------------------------------------------------------------
create table "users"
(
  id bigint primary key default nextval('user_id_seq'),
  name varchar unique,
  password varchar,
  method varchar
);

-- ---------------------------------------------------------------------
create table "namespaces"
(
  id bigint primary key default nextval('namespaces_id_seq'),
  name varchar unique
);

-- ---------------------------------------------------------------------
create table "namespace_members"
(
  namespace_id bigint references "namespaces" (id) on delete cascade,
  array_id bigint references "array" (id) on delete cascade,

  primary key(namespace_id, array_id)
);

-- ---------------------------------------------------------------------
-- Initialization
-- ---------------------------------------------------------------------
insert into users (name, password, method) values (
    'root',
    'eUCUk3B57IVO9ZfJB6CIEHl/0lxrWg/7PV8KytUNY6kPLhTX2db48GHGHoizKyH+\nuGkCfNTYZrJgKzjWOhjuvg==',
    'raw');

insert into namespaces (name, id) values ('public', 1);

-- ---------------------------------------------------------------------------
-- Drop the triggers that checks attribute and dimension names do not conflict
-- See ticket:3676.
-- ---------------------------------------------------------------------------
drop trigger if exists check_array_repeated_dim_attr_names on array_attribute;
drop trigger if exists check_array_repeated_attr_dim_names on array_dimension;
drop function if exists check_no_array_dupes();

-- ---------------------------------------------------------------------
-- CLUSTER VERSION UPDATE
-- ---------------------------------------------------------------------
update "cluster" set metadata_version = 3;
