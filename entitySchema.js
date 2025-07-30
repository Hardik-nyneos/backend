const { Client } = require("pg");

require("dotenv").config();
const db = new Client({
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  ssl: {
    rejectUnauthorized: false,
  },
});

db.connect()
  .then(() => {
    console.log("✅.");
    return db.query(`
  CREATE TABLE masterEntity (
    entity_id VARCHAR(100) PRIMARY KEY,
    registration_number VARCHAR(250),
    pan_gst VARCHAR(250),
    legal_entity_identifier VARCHAR(250),
    tax_identification_number VARCHAR(250),
    default_currency VARCHAR(50),
    associated_business_units TEXT[],
    reporting_currency VARCHAR(50),
    entity_name VARCHAR(250) NOT NULL,
    address TEXT,
    contact_phone VARCHAR(50),
    contact_email VARCHAR(250),
    unique_identifier VARCHAR(250) UNIQUE,
    legal_entity_type VARCHAR(250),
    fx_trading_authority VARCHAR(250),
    internal_fx_trading_limit VARCHAR(250),
    associated_treasury_contact VARCHAR(250),
    is_top_level_entity BOOLEAN,
    is_deleted BOOLEAN DEFAULT false,
    approval_status VARCHAR(50) DEFAULT 'Pending',
    parentName VARCHAR(250),
    level VARCHAR(50)
  );
`);
  })
  .then(() => {
    // Create entityRelationships table
    return db.query(`
      CREATE TABLE entityRelationships (
        relationship_id SERIAL PRIMARY KEY,
        parent_entity_id VARCHAR(10) NOT NULL REFERENCES masterEntity(entity_id),
        child_entity_id VARCHAR(10) NOT NULL UNIQUE REFERENCES masterEntity(entity_id),
        status VARCHAR(50) DEFAULT 'Active' CHECK (status IN ('Active', 'Suspended', 'Inactive')),
        CHECK (parent_entity_id <> child_entity_id)
      );
    `);
  })
  .then(() => {
    console.log("✅ Created masterEntity and entityRelationships tables");
    db.end();
  })
  .catch((err) => {
    console.error("❌ Error:", err.stack);
    db.end();
  });
