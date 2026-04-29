CREATE TABLE IF NOT EXISTS items (
    sku TEXT PRIMARY KEY,
    stock INT NOT NULL
);

INSERT INTO items (sku, stock) VALUES
    ('coffee', 42),
    ('mug', 11),
    ('beans', 100),
    ('grinder', 7),
    ('filter', 250)
ON CONFLICT (sku) DO NOTHING;
