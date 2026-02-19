-- Genesis Tree — Universal Human Lineage Graph
-- PostgreSQL Schema

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ─────────────────────────────────────────────
-- PERSONS
-- ─────────────────────────────────────────────
CREATE TABLE persons (
    id SERIAL PRIMARY KEY,
    name VARCHAR(512) NOT NULL,
    also_known_as TEXT[], -- aliases
    type VARCHAR(32) DEFAULT 'human', -- human | mythological | genesis
    genesis_code VARCHAR(16), -- G1, G2 ... if unresolved root
    era VARCHAR(128), -- 'Ancient', 'Medieval', 'Modern', 'Mythological'
    approx_birth_year INTEGER, -- negative = BCE
    approx_death_year INTEGER,
    gender VARCHAR(16), -- male | female | unknown | deity
    wikidata_id VARCHAR(32), -- Q-code for deduplication
    wikipedia_slug VARCHAR(512),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    is_genesis BOOLEAN DEFAULT FALSE,
    agent_researched BOOLEAN DEFAULT FALSE
);

CREATE INDEX idx_persons_name ON persons USING gin(to_tsvector('english', name));
CREATE INDEX idx_persons_wikidata ON persons(wikidata_id);
CREATE INDEX idx_persons_genesis ON persons(is_genesis);

-- ─────────────────────────────────────────────
-- CATEGORIES (tags on persons)
-- ─────────────────────────────────────────────
CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(128) UNIQUE NOT NULL, -- 'Greek Gods', 'Roman Emperors', etc.
    parent_category_id INTEGER REFERENCES categories(id),
    icon VARCHAR(64), -- emoji or icon name
    display_order INTEGER DEFAULT 0
);

CREATE TABLE person_categories (
    person_id INTEGER REFERENCES persons(id) ON DELETE CASCADE,
    category_id INTEGER REFERENCES categories(id) ON DELETE CASCADE,
    PRIMARY KEY (person_id, category_id)
);

-- ─────────────────────────────────────────────
-- RELATIONSHIPS (the core graph)
-- ─────────────────────────────────────────────
CREATE TABLE relationships (
    id SERIAL PRIMARY KEY,
    child_id INTEGER REFERENCES persons(id) ON DELETE CASCADE,
    parent_id INTEGER REFERENCES persons(id) ON DELETE CASCADE,
    parent_type VARCHAR(8) NOT NULL, -- 'father' | 'mother'
    confidence NUMERIC(5,2) DEFAULT 50.0, -- 0 to 100
    is_primary BOOLEAN DEFAULT TRUE, -- highest confidence branch
    is_branch BOOLEAN DEFAULT FALSE, -- alternate possibility
    branch_group INTEGER DEFAULT 0, -- groups competing branches together
    verified_by_user BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(child_id, parent_id, parent_type)
);

CREATE INDEX idx_rel_child ON relationships(child_id);
CREATE INDEX idx_rel_parent ON relationships(parent_id);

-- ─────────────────────────────────────────────
-- SOURCES (per relationship)
-- ─────────────────────────────────────────────
CREATE TABLE sources (
    id SERIAL PRIMARY KEY,
    relationship_id INTEGER REFERENCES relationships(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    title VARCHAR(512),
    source_type VARCHAR(32), -- 'wikipedia' | 'wikidata' | 'news' | 'user'
    retrieved_at TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────────
-- AGENT QUEUE
-- ─────────────────────────────────────────────
CREATE TABLE agent_queue (
    id SERIAL PRIMARY KEY,
    person_id INTEGER REFERENCES persons(id) ON DELETE CASCADE,
    direction VARCHAR(8) DEFAULT 'both', -- 'up' | 'down' | 'both'
    priority INTEGER DEFAULT 50, -- higher = sooner
    status VARCHAR(16) DEFAULT 'pending', -- pending | processing | done | failed
    attempts INTEGER DEFAULT 0,
    last_attempt TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_queue_status_priority ON agent_queue(status, priority DESC);

-- ─────────────────────────────────────────────
-- MERGE LOG (genesis resolution events)
-- ─────────────────────────────────────────────
CREATE TABLE merge_log (
    id SERIAL PRIMARY KEY,
    genesis_person_id INTEGER REFERENCES persons(id),
    genesis_code VARCHAR(16),
    merged_into_person_id INTEGER REFERENCES persons(id),
    confidence_at_merge NUMERIC(5,2),
    merged_at TIMESTAMP DEFAULT NOW(),
    notes TEXT
);

-- ─────────────────────────────────────────────
-- AGENT ACTIVITY LOG
-- ─────────────────────────────────────────────
CREATE TABLE agent_log (
    id SERIAL PRIMARY KEY,
    person_id INTEGER REFERENCES persons(id),
    person_name VARCHAR(512),
    action VARCHAR(64), -- 'discovered', 'linked', 'merged', 'failed'
    detail TEXT,
    logged_at TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────────
-- SEED CATEGORIES
-- ─────────────────────────────────────────────
INSERT INTO categories (name, icon, display_order) VALUES
('Mythological', '⚡', 1),
('Human', '👤', 2),
('Ancient', '🏛️', 3),
('Medieval', '⚔️', 4),
('Modern', '🌍', 5),
('Royalty & Dynasties', '👑', 6),
('Religion & Scripture', '📖', 7),
('Science & Philosophy', '🔬', 8);

-- Mythological subcategories
INSERT INTO categories (name, icon, parent_category_id, display_order)
SELECT name, icon, (SELECT id FROM categories WHERE name='Mythological'), display_order FROM (VALUES
    ('Greek Gods', '🏺', 1),
    ('Norse Gods', '🪓', 2),
    ('Hindu Deities', '🕉️', 3),
    ('Egyptian Gods', '𓂀', 4),
    ('Mesopotamian Gods', '🌙', 5),
    ('Roman Gods', '🦅', 6),
    ('Celtic Gods', '🍀', 7),
    ('Aztec Gods', '🌞', 8)
) AS t(name, icon, display_order);

-- Ancient subcategories
INSERT INTO categories (name, icon, parent_category_id, display_order)
SELECT name, icon, (SELECT id FROM categories WHERE name='Ancient'), display_order FROM (VALUES
    ('Egyptian Pharaohs', '𓇳', 1),
    ('Roman Emperors', '🦅', 2),
    ('Greek Kings', '🏛️', 3),
    ('Sumerian Kings', '📜', 4),
    ('Persian Kings', '🔥', 5),
    ('Biblical Figures', '✝️', 6),
    ('Quranic Figures', '☪️', 7),
    ('Vedic Figures', '🕉️', 8)
) AS t(name, icon, display_order);

-- Royalty subcategories
INSERT INTO categories (name, icon, parent_category_id, display_order)
SELECT name, icon, (SELECT id FROM categories WHERE name='Royalty & Dynasties'), display_order FROM (VALUES
    ('British Royals', '🇬🇧', 1),
    ('French Royalty', '🇫🇷', 2),
    ('Ottoman Dynasty', '🌙', 3),
    ('Mughal Dynasty', '🕌', 4),
    ('Chinese Dynasties', '🐉', 5),
    ('Japanese Royals', '🌸', 6),
    ('Mongol Khans', '🏹', 7),
    ('Habsburg Dynasty', '⚜️', 8)
) AS t(name, icon, display_order);

-- Human subcategories
INSERT INTO categories (name, icon, parent_category_id, display_order)
SELECT name, icon, (SELECT id FROM categories WHERE name='Human'), display_order FROM (VALUES
    ('Americans', '🇺🇸', 1),
    ('Europeans', '🇪🇺', 2),
    ('South Asians', '🇮🇳', 3),
    ('East Asians', '🀄', 4),
    ('Middle Eastern', '🌙', 5),
    ('Africans', '🌍', 6),
    ('Latin Americans', '🌎', 7),
    ('Notable Families', '🏠', 8)
) AS t(name, icon, display_order);

-- Notable Families
INSERT INTO categories (name, icon, parent_category_id, display_order)
SELECT name, icon, (SELECT id FROM categories WHERE name='Notable Families'), display_order FROM (VALUES
    ('Ambani Family', '💎', 1),
    ('Rockefeller Family', '🛢️', 2),
    ('Rothschild Family', '🏦', 3),
    ('Windsor Family', '👑', 4),
    ('Kennedy Family', '🇺🇸', 5),
    ('Medici Family', '🎨', 6),
    ('Nehru-Gandhi Family', '🇮🇳', 7),
    ('Bush Family', '🦅', 8)
) AS t(name, icon, display_order);

-- ─────────────────────────────────────────────
-- SEED GENESIS BLOCK (G0 — the universal root)
-- ─────────────────────────────────────────────
INSERT INTO persons (name, type, is_genesis, genesis_code, era, gender)
VALUES ('Genesis Root', 'genesis', TRUE, 'G0', 'Unknown', 'unknown');
