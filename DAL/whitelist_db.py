import psycopg2
from typing import List, Optional

# Mesma configuração do load_hostnames.py
DB_CONFIG = {
    "host": "localhost",
    "database": "validador_hostnames",
    "user": "postgres",
    "password": "postgres"
}

class WhitelistDB:
    def __init__(self):
        self.config = DB_CONFIG

    def check_hostname(self, hostname: str) -> dict:
        """
        Verifica se um hostname é permitido (Busca Otimizada B-Tree).
        Suporta múltiplos níveis de wildcard (Recursividade).
        """
        hostname = hostname.strip().lower()
        
        conn = psycopg2.connect(**self.config)
        try:
            with conn.cursor() as cur:
                # 1. Tenta Match Exato (is_wildcard = FALSE)
                cur.execute(
                    "SELECT hostname FROM allowed_hosts WHERE hostname = %s AND is_wildcard = FALSE", 
                    (hostname,)
                )
                res = cur.fetchone()
                if res:
                    return {"allowed": True, "type": "exact", "match": res[0], "reason": "Match exato na whitelist"}

                # 2. Tenta Match de Wildcard Recursivo (Subindo os níveis)
                # Ex: a.b.google.com -> tenta b.google.com -> tenta google.com
                parts = hostname.split('.')
                while len(parts) > 1:
                    parts.pop(0) # Remove a subparte mais à esquerda
                    parent_domain = ".".join(parts)
                    
                    cur.execute(
                        "SELECT hostname FROM allowed_hosts WHERE hostname = %s AND is_wildcard = TRUE", 
                        (parent_domain,)
                    )
                    res_w = cur.fetchone()
                    if res_w:
                        return {
                            "allowed": True, 
                            "type": "wildcard", 
                            "match": f"*.{res_w[0]}",
                            "reason": f"Permitido via regra wildcard de {res_w[0]}"
                        }

        except Exception as e:
            print(f"❌ Erro na consulta ao banco: {e}")
        finally:
            conn.close()

        return {"allowed": False, "type": None, "match": None, "reason": "Não encontrado na whitelist"}

    def save_probe_history(self, hostname: str, status: str, latency: float, ips: List[str], reason: str = ""):
        """
        Salva o histórico de checks (Requisito 5.2).
        ips: lista de strings (ex: ['1.1.1.1', '8.8.8.8'])
        """
        sql = """
            INSERT INTO probe_history (hostname, status, latency_ms, resolved_ips, reason)
            VALUES (%s, %s, %s, %s, %s)
        """
        conn = psycopg2.connect(**self.config)
        try:
            with conn.cursor() as cur:
                # O psycopg2 converte a lista 'ips' automaticamente para o formato ARRAY do Postgres
                cur.execute(sql, (hostname, status, latency, ips, reason))
                conn.commit()
        except Exception as e:
            print(f"❌ Erro ao salvar histórico: {e}")
        finally:
            conn.close()