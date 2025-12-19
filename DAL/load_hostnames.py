import os
import time
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# --- CONFIGURAÇÃO ---
DB_CONFIG = {
    "host": "localhost",
    "database": "validador_hostnames",
    "user": "postgres",
    "password": "postgres"
}

HOSTS_FILE = os.path.join(os.path.dirname(__file__), "hostnames.txt")

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def init_db():
    """Configura o schema com foco em performance (B-Tree) e Metadados."""
    print("🛠️  Configurando estrutura do banco de dados...")
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # 1. Tabela Principal com Metadados (Fonte e Timestamps)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS allowed_hosts (
                id SERIAL PRIMARY KEY,
                hostname TEXT NOT NULL,
                is_wildcard BOOLEAN DEFAULT FALSE,
                source TEXT DEFAULT 'import_manual',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_host_type UNIQUE (hostname, is_wildcard)
            );
        """)

        # 2. B-Tree Composto (O segredo da performance de Nível 4)
        # Busca exata e busca de wildcard em uma única varredura de árvore
        cur.execute("CREATE INDEX IF NOT EXISTS idx_host_wildcard_btree ON allowed_hosts USING btree (hostname, is_wildcard);")

        # 3. Tabela de Histórico (Requisito 5.2)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS probe_history (
                id SERIAL PRIMARY KEY,
                hostname TEXT NOT NULL,
                status VARCHAR(20),  -- VALID, UNKNOWN, DISALLOWED
                reason TEXT,         -- Motivo (ex: 'Match em *.google.com')
                resolved_ips TEXT[], -- Array nativo do Postgres para múltiplos IPs
                latency_ms REAL,     -- Latência (RTT)
                checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_history_host_btree ON probe_history USING btree (hostname);")

        conn.commit()
        print("✅ Banco pronto para 1 milhão de registros.")
    except Exception as e:
        print(f"❌ Erro no init_db: {e}")
        conn.rollback()
    finally:
        conn.close()

def load_hostnames():
    """Carga massiva com metadados e correção de timestamp."""
    if not os.path.exists(HOSTS_FILE):
        print("Arquivo hostnames.txt não encontrado!")
        return

    print("🚀 Iniciando carga massiva...")
    start_time = time.time()
    conn = get_db_connection()
    cur = conn.cursor()
    
    batch_size = 10000
    batch_data = []
    total_processed = 0
    source_tag = "batch_import_2025_001" # Metadado da fonte

    try:
        with open(HOSTS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                raw = line.strip().lower()
                if not raw or raw.startswith("#"): continue

                # Normalização
                if raw.startswith("*."):
                    host, wildcard = raw[2:], True
                else:
                    host, wildcard = raw, False
                
                # Prepara a tupla com metadados (Hostname, Wildcard, Fonte, Timestamp)
                # Usamos replace(microsecond=0) para evitar o erro do Postgres
                now = datetime.now().replace(microsecond=0)
                batch_data.append((host, wildcard, source_tag, now))

                if len(batch_data) >= batch_size:
                    execute_values(cur, """
                        INSERT INTO allowed_hosts (hostname, is_wildcard, source, created_at)
                        VALUES %s ON CONFLICT (hostname, is_wildcard) DO NOTHING
                    """, batch_data)
                    total_processed += len(batch_data)
                    batch_data = []
                    print(f"📥 Processados: {total_processed}...", end='\r')

            # Sobras
            if batch_data:
                execute_values(cur, """
                    INSERT INTO allowed_hosts (hostname, is_wildcard, source, created_at)
                    VALUES %s ON CONFLICT (hostname, is_wildcard) DO NOTHING
                """, batch_data)
                total_processed += len(batch_data)

        conn.commit()
        print(f"\n✨ Sucesso! {total_processed} registros em {time.time()-start_time:.2f}s")
    except Exception as e:
        print(f"\n❌ Erro na carga: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    init_db()
    load_hostnames()