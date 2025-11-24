CREATE TABLE IF NOT EXISTS texts (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sections (
    id SERIAL PRIMARY KEY,
    text_id INTEGER NOT NULL REFERENCES texts(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    section_number INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_text_section UNIQUE (text_id, section_number)
);

CREATE INDEX IF NOT EXISTS idx_sections_text_id ON sections(text_id);
CREATE INDEX IF NOT EXISTS idx_sections_section_number ON sections(section_number);
CREATE INDEX IF NOT EXISTS idx_texts_name ON texts(name);
