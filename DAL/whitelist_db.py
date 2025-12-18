import psycopg2
from typing import List, Optional

# Configurações de conexão (devem ser as mesmas do load_hostnames.py)
DB_CONFIG = {
    "host": "localhost",
    "database": "validador_hostnames",
    "user": "postgres",
    "password": "postgres"
}

class WhitelistDB:
    def __init__(self):
        self.config = DB_CONFIG

    def _execute_query(self, sql: str, params: tuple):
        """Método auxiliar para gerenciar conexões com o Postgres."""
        conn = psycopg2.connect(**self.config)
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
        finally:
            conn.close()

    def check_hostname(self, hostname: str) -> dict:
        """
        Verifica se um hostname é permitido.
        Implementa lógica de match exato e wildcard (Nível 4).
        """
        hostname = hostname.strip().lower()
        
        # 1. Tenta Match Exato
        # Busca por registros onde is_wildcard é Falso
        exact_query = "SELECT hostname FROM allowed_hosts WHERE hostname = %s AND is_wildcard = FALSE LIMIT 1"
        res = self._execute_query(exact_query, (hostname,))
        
        if res:
            return {"allowed": True, "type": "exact", "match": res[0][0]}

        # 2. Tenta Match de Wildcard (Domínio Pai)
        # Se recebemos 'app.google.com', testamos se existe 'google.com' com is_wildcard = True
        parts = hostname.split('.')
        if len(parts) > 1:
            parent_domain = ".".join(parts[1:])
            wildcard_query = "SELECT hostname FROM allowed_hosts WHERE hostname = %s AND is_wildcard = TRUE LIMIT 1"
            res = self._execute_query(wildcard_query, (parent_domain,))
            
            if res:
                return {"allowed": True, "type": "wildcard", "match": f"*.{res[0][0]}"}

        return {"allowed": False, "type": None, "match": None}

    def save_probe_history(self, hostname: str, status: str, latency: int, ips: List[str]):
        """
        Salva o resultado do probe na tabela separada de histórico (Requisito 5.2).
        """
        sql = """
            INSERT INTO probe_history (hostname, status, latency_ms, resolved_ips)
            VALUES (%s, %s, %s, %s)
        """
        conn = psycopg2.connect(**self.config)
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (hostname, status, latency, ips))
                conn.commit()
        finally:
            conn.close()