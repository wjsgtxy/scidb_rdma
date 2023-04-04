--upgrade from 3 to 4

-- ---------------------------------------------------------------------
-- DROP VIEWS
-- ---------------------------------------------------------------------
drop view if exists namespace_arrays cascade;
drop view if exists public_arrays cascade;
drop view if exists namespace_members_view cascade;

-- ---------------------------------------------------------------------
-- UPDATE SEQUENCES
-- ---------------------------------------------------------------------
create sequence "array_distribution_id_seq" minvalue 0 start with 0;
alter sequence "user_id_seq" minvalue 1 start with 2;
create sequence "role_permissions_id_seq" minvalue 1 start with 1;
create sequence "role_namespace_permissions_id_seq";
create sequence "role_id_seq" minvalue 1 start with 2;

-- ---------------------------------------------------------------------
-- UPDATE TABLES and FUNCTIONS
-- ---------------------------------------------------------------------
create table "array_distribution"
(
  id bigint primary key default nextval('array_distribution_id_seq'),
  partition_function integer,
  partition_state varchar,
  redundancy integer
);

create table "roles"
(
  id bigint primary key default nextval('role_id_seq'),
  name varchar unique
);

create table "role_members"
(
  role_id bigint references "roles" (id) on delete cascade,
  user_id bigint references "users" (id) on delete cascade,

  primary key(role_id, user_id)
);

create table "role_namespace_permissions"
(
  id bigint primary key default nextval('role_namespace_permissions_id_seq'),
  role_id bigint references "roles" (id) on delete cascade,
  namespace_id bigint references "namespaces" (id) on delete cascade,
  permissions varchar,

  unique(role_id, namespace_id)
);

-- ---------------------------------------------------------------------
alter table "array"
  add column distribution_id bigint references "array_distribution" (id),
  drop column partitioning_schema,
  alter column name type varchar,
  add unique (id, name);


drop table "array_partition";
drop sequence "partition_id_seq";

alter table "instance"
      rename column path to base_path;

alter table "instance"
      alter column instance_id drop default,
      add column membership_id bigint default 0,
      add column server_id integer,
      add column server_instance_id integer;

update "instance" set membership_id=0;
update "instance" set online_since='now';
update "instance" set server_id=substring(base_path,'.+/([0-9]+)/[0-9]+')::integer;
update "instance" set server_instance_id=substring(base_path,'.+/[0-9]+/([0-9])+')::integer;
update "instance" set base_path=substring(base_path,'(.+)/[0-9]+/[0-9]+');
update "instance" set instance_id=((server_id << 32) | (instance_id & (x'00FFFFFFFF'::bigint)));

alter table "instance"
      add constraint instance_host_key UNIQUE(host,port),
      add constraint instance_server_id_server_instance_id UNIQUE(server_id,server_instance_id),
      add constraint instance_host_server_id_server_instance_id UNIQUE(host,server_id,server_instance_id);

CREATE FUNCTION check_server_id_host(integer,varchar)
RETURNS BIGINT AS
$$
   SELECT count(*) from (select true from "instance" where server_id=$1 and host<>$2 limit 1) as X;
$$ LANGUAGE SQL IMMUTABLE;

alter table "instance"
add constraint instance_server_id_host_unique CHECK (check_server_id_host(server_id,host)=0);

CREATE FUNCTION check_base_path(varchar)
RETURNS BIGINT AS
$$
   SELECT count(*) from (select true from "instance" where base_path<>$1 limit 1) as X;
$$ LANGUAGE SQL IMMUTABLE;

alter table "instance"
add constraint instance_base_path_non_unique CHECK (check_base_path(base_path)=0);

alter table "array_version_lock"
      drop constraint array_version_lock_array_name_key,
      drop column instance_role,
      add column coordinator_id bigint references "instance" (instance_id),
      add column namespace_name varchar,
      add constraint array_version_lock_array_name_key UNIQUE(namespace_name, array_name, coordinator_id, query_id, instance_id),
      add constraint array_version_lock_instance_id_fkey FOREIGN KEY (instance_id) REFERENCES "instance" (instance_id) MATCH FULL;


create table "membership"
(
  id bigint primary key default 0
);

-- ---------------------------------------------------------------------

CREATE FUNCTION check_uaid_in_array(bigint)
RETURNS BIGINT AS
$$
   SELECT count(*) from (SELECT true FROM "array" WHERE id = $1 and name not like '%@%' limit 1) as X
$$ LANGUAGE SQL IMMUTABLE;

-- ---------------------------------------------------------------------
create table "array_residency"
(
  array_id bigint CHECK (check_uaid_in_array(array_id)>0),
  instance_id bigint references "instance" (instance_id),
  primary key(array_id, instance_id)
);

-- ---------------------------------------------------------------------
-- Initialization
-- ---------------------------------------------------------------------
-- ASSUMING func=psHashPartitioned, state='', redundancy=0
insert into "array_distribution" (id,partition_function,partition_state,redundancy) (select distinct A.id, 1,'',0 from "array" as A where A.name not like '%@%');
update "array" set distribution_id=id where name not like '%@%';
update "array" set distribution_id=AV.array_id from "array_version" as AV where id=AV.version_array_id;

insert into "membership" (id) values (0);
insert into "array_residency" (array_id,instance_id) (select distinct A.id, I.instance_id from "array" as A , "instance" as I where A.name not like '%@%');

update users set password=E'eUCUk3B57IVO9ZfJB6CIEHl/0lxrWg/7PV8KytUNY6kPLhTX2db48GHGHoizKyH+\nuGkCfNTYZrJgKzjWOhjuvg==' where name='root';
update users set id=1 where name='root';
insert into roles (id, name) values (1, 'root');
insert into role_members (role_id, user_id) values(1,1);

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


-- ---------------------------------------------------------------------
-- CLUSTER VERSION UPDATE
-- ---------------------------------------------------------------------
update "cluster" set metadata_version = 4;
