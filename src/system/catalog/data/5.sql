-- upgrade from 4 to 5

-- DBA is now "scidbadmin", not "root".
UPDATE users SET name = 'scidbadmin' WHERE id = 1;

-- User accounts table needs salt column.
ALTER TABLE users ADD COLUMN salt varchar;


-- ---------------------------------------------------------------------
-- CLUSTER VERSION UPDATE
-- ---------------------------------------------------------------------
UPDATE cluster SET metadata_version = 5;
