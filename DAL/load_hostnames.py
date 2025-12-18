import os
import time
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# --- CONFIGURAÇÃO DO BANCO DE DADOS ---
# Ajuste conforme suas credenciais locais ou do Docker
DB_HOST = "localhost"
DB_NAME = "validador_hostnames"
DB_USER = "postgres"
DB_PASS = "postgres"

# Caminho do arquivo de texto (1 milhão de linhas)
HOSTS_FILE = os.path.join(os.path.dirname(__file__), "hostnames.txt")


def get_db_connection():
    """Estabelece conexão com o PostgreSQL."""
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    return conn


def init_db():
    """
    Cria as tabelas e índices necessários (Schema Migration).
    Executa a separação entre Whitelist e Histórico (Tema C).
    """
    print("Verificando estrutura do banco de dados...")
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        
        # 1. Tabela de Whitelist (Normalizada)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS allowed_hosts (
                id SERIAL PRIMARY KEY,
                hostname TEXT NOT NULL,
                is_wildcard BOOLEAN DEFAULT FALSE,
                source TEXT DEFAULT 'carga_inicial',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                
                -- Restrição única composta: evita duplicar 'google.com' (exato) e 'google.com' (wildcard)
                CONSTRAINT uq_hostname_type UNIQUE (hostname, is_wildcard)
            );
        """)

        # 2. Índices B-Tree para Performance (Critério de Nível 4)
        # Otimiza a busca exata e a busca do pai (wildcard)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_allowed_hosts_hostname ON allowed_hosts (hostname);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_allowed_hosts_wildcard ON allowed_hosts (is_wildcard);")

        # 3. Tabela de Histórico/Probes (Separada da Whitelist)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS probe_history (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- Requer Postgres 13+
                hostname TEXT NOT NULL,
                status VARCHAR(20), -- VALID, INVALID, UNKNOWN
                latency_ms INTEGER,
                resolved_ips INET[], -- Array de IPs nativo do Postgres
                checked_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Índice para histórico (opcional, para relatórios futuros)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_probe_history_host ON probe_history (hostname);")

        conn.commit()
        print("✅ Tabelas e índices configurados com sucesso.")
    except Exception as e:
        print(f"❌ Erro ao criar banco: {e}")
        conn.rollback()
    finally:
        conn.close()


def load_hostnames():
    """
    Lê o arquivo TXT e carrega no PostgreSQL usando Batch Insert.
    Trata a lógica de *. (wildcard) removendo o prefixo e setando a flag.
    """
    if not os.path.exists(HOSTS_FILE):
        print(f"Arquivo não encontrado: {HOSTS_FILE}")
        print("Certifique-se de colocar o arquivo 'hostnames.txt' na mesma pasta.")
        return

    print("Iniciando carga de dados...")
    start_time = time.time()
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    batch_size = 10000
    batch_data = []
    total_inserted = 0

    # SQL de Inserção Otimizada
    # Gera uma data aleatória nos últimos 30 dias para simular dados realistas (Requisito 5.1)
    sql = """
        INSERT INTO allowed_hosts (hostname, is_wildcard, source, created_at)
        VALUES %s
        ON CONFLICT (hostname, is_wildcard) DO NOTHING
    """

    try:
        with open(HOSTS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                raw_line = line.strip().lower()
                
                if not raw_line or raw_line.startswith("#"):
                    continue

                # Lógica de Normalização:
                # Se for "*.google.com", salva "google.com" e is_wildcard=True
                # Se for "google.com", salva "google.com" e is_wildcard=False
                if raw_line.startswith("*."):
                    clean_host = raw_line[2:] # Remove o *.
                    is_wildcard = True
                else:
                    clean_host = raw_line
                    is_wildcard = False
                
                # Adiciona tupla à lista (o SQL gera a data, aqui passamos um placeholder se necessário, 
                # mas vamos deixar o Python gerar a estrutura para o execute_values)
                # Para simplificar e usar a função do Postgres para data, vamos injetar valores fixos aqui
                # e deixar o SQL cuidar do resto ou passar tudo do python.
                
                # Vamos passar: (hostname, is_wildcard, source, created_at expression handled below)
                # Nota: execute_values espera valores literais. Vamos simplificar.
                
                batch_data.append((clean_host, is_wildcard, 'lote_massivo'))

                # Quando o lote encher, executa
                if len(batch_data) >= batch_size:
                    now = datetime.now()
                    # Criamos a lista de tuplas com o timestamp já formatado
                    data_to_insert = [(h, w, s, now) for h, w, s in batch_data]
                    
                    execute_values(cur, """
                        INSERT INTO allowed_hosts (hostname, is_wildcard, source, created_at)
                        VALUES %s
                        ON CONFLICT (hostname, is_wildcard) DO NOTHING
                    """, data_to_insert)
                    
                    total_inserted += len(batch_data)
                    batch_data = [] 
                    print(f"Processados: {total_inserted}...", end='\r')

            # Insere o resto que sobrou
            if batch_data:
                execute_values(cur, """
                    INSERT INTO allowed_hosts (hostname, is_wildcard, source, created_at)
                    VALUES %s
                    ON CONFLICT (hostname, is_wildcard) DO NOTHING
                """, [(h, w, s, psycopg2.TimestampFromTicks(time.time())) for h, w, s in batch_data])
                total_inserted += len(batch_data)

        conn.commit()
        duration = time.time() - start_time
        print(f"\n✅ Carga finalizada! {total_inserted} registros processados em {duration:.2f} segundos.")
        
    except Exception as e:
        print(f"\n❌ Erro durante a carga: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    init_db()
    load_hostnames()