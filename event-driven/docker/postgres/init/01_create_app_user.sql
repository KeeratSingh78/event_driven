-- ─────────────────────────────────────────────────────────────────────────────
-- 01_create_app_user.sql
-- Runs once during the first `docker compose up` to create the dedicated
-- application-level database role.  The superuser is never used by the app.
--
-- Environment variables are not available inside init SQL scripts, so the
-- credentials here must match what you set in .env / docker-compose.yml.
-- If you change them, bring the volume down first: docker compose down -v
-- ─────────────────────────────────────────────────────────────────────────────

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'event_app') THEN
        CREATE ROLE event_app WITH LOGIN PASSWORD 'keerat78';
    ELSE
        ALTER ROLE event_app WITH PASSWORD 'keerat78';
    END IF;
END
$$;

-- Grant ownership/privileges on database and public schema for PG 15+
GRANT ALL PRIVILEGES ON DATABASE event_platform TO event_app;
GRANT ALL ON SCHEMA public TO event_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO event_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO event_app;
