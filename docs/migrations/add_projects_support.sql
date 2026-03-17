-- Migration script to add Projects support
-- Run this script on your main database (not customer databases)

-- Step 1: Create projects table
CREATE TABLE IF NOT EXISTS projects (
    id SERIAL PRIMARY KEY,
    project_name VARCHAR(255) NOT NULL,
    description TEXT,
    customer_id INTEGER NOT NULL REFERENCES customer(cust_id) ON DELETE CASCADE,
    created_by_id INTEGER,
    created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_by_id INTEGER,
    is_active BOOLEAN DEFAULT TRUE
);

-- Create index on customer_id for faster lookups
CREATE INDEX IF NOT EXISTS idx_projects_customer_id ON projects(customer_id);
CREATE INDEX IF NOT EXISTS idx_projects_is_active ON projects(is_active);

-- Step 2: Add project_id column to canvas table (nullable for backward compatibility)
ALTER TABLE canvas ADD COLUMN IF NOT EXISTS project_id INTEGER REFERENCES projects(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_canvas_project_id ON canvas(project_id);

-- Step 3: For each customer database, add project_id to GENERAL.source and GENERAL.destination
-- Note: This needs to be run per customer database
-- Example for a customer database:
-- 
-- ALTER TABLE "GENERAL".source ADD COLUMN IF NOT EXISTS project_id INTEGER;
-- ALTER TABLE "GENERAL".destination ADD COLUMN IF NOT EXISTS project_id INTEGER;
-- CREATE INDEX IF NOT EXISTS idx_source_project_id ON "GENERAL".source(project_id);
-- CREATE INDEX IF NOT EXISTS idx_destination_project_id ON "GENERAL".destination(project_id);

-- Step 4: Create default project for each existing customer (optional - for migration)
-- This assigns all existing canvases/sources/destinations to a default project
-- 
-- DO $$
-- DECLARE
--     cust_record RECORD;
--     default_project_id INTEGER;
-- BEGIN
--     FOR cust_record IN SELECT cust_id FROM customer LOOP
--         -- Create default project for this customer
--         INSERT INTO projects (project_name, description, customer_id, is_active)
--         VALUES ('Default Project', 'Default project for existing data', cust_record.cust_id, TRUE)
--         RETURNING id INTO default_project_id;
--         
--         -- Assign existing canvases to default project
--         UPDATE canvas SET project_id = default_project_id WHERE customer_id = cust_record.cust_id AND project_id IS NULL;
--         
--         -- Note: For sources and destinations, you'll need to update them in each customer database
--     END LOOP;
-- END $$;

