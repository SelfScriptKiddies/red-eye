CREATE TABLE tag_name (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    color SMALLINT -- Frontender said to contain only HUE degree
);