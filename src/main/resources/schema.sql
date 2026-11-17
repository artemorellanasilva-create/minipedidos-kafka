
-- ==========================================
-- MINIPEDIDOS V2 - SESION 3 - PARTE 3
-- BASE DE DATOS DE AUTENTICACION
-- ==========================================

-- ==========================================
-- 1. TABLA DE USUARIOS
-- ==========================================

CREATE TABLE IF NOT EXISTS users (

    username VARCHAR(50) NOT NULL PRIMARY KEY,

    password VARCHAR(500) NOT NULL,

    enabled BOOLEAN NOT NULL DEFAULT TRUE

);

-- ==========================================
-- 2. TABLA DE ROLES Y AUTORIZACIONES
-- ==========================================

CREATE TABLE IF NOT EXISTS authorities (

    username VARCHAR(50) NOT NULL,

    authority VARCHAR(50) NOT NULL,

    CONSTRAINT fk_authorities_users
        FOREIGN KEY (username)
        REFERENCES users(username),

    CONSTRAINT uq_authorities
        UNIQUE (username, authority)

);

-- ==========================================
-- 3. INDICE PARA CONSULTAR LOS ROLES
-- ==========================================

CREATE INDEX IF NOT EXISTS idx_authorities_username
    ON authorities(username);
