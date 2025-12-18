import psycopg2
import time
import random
import string

def gerar_aleatorio():
    """Gera um hostname que provavelmente não existe no banco."""
    return "".join(random.choice(string.ascii_lowercase) for _ in range(10)) + ".com"

def testar_massa():
    conn_params = {
        "host": "localhost",
        "database": "validador_hostnames",
        "user": "postgres",
        "password": "postgres"
    }
    
    # Lista para testar: 20 conhecidos (que sabemos que estão lá) + 80 aleatórios
    hosts_para_testar = [
        "google.com", "mail.google.com", "example.com", "static.cdn.net", 
        "uol.com.br", "esporte.uol.com.br", "aws.amazon.com", "github.com"
    ]
    while len(hosts_para_testar) < 100:
        hosts_para_testar.append(gerar_aleatorio())

    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()
    
    print(f"🚀 Iniciando validação de {len(hosts_para_testar)} hostnames em um banco de 1 milhão...")
    print("-" * 50)
    
    inicio_total = time.perf_counter()
    permitidos = 0
    bloqueados = 0

    for host in hosts_para_testar:
        # Lógica otimizada de busca
        cur.execute("SELECT 1 FROM allowed_hosts WHERE hostname = %s AND is_wildcard = FALSE", (host,))
        if cur.fetchone():
            permitidos += 1
        else:
            parts = host.split('.')
            found = False
            while len(parts) > 1:
                parts.pop(0)
                pai = ".".join(parts)
                cur.execute("SELECT 1 FROM allowed_hosts WHERE hostname = %s AND is_wildcard = TRUE", (pai,))
                if cur.fetchone():
                    found = True
                    break
            if found:
                permitidos += 1
            else:
                bloqueados += 1

    fim_total = time.perf_counter()
    tempo_total_ms = (fim_total - inicio_total) * 1000

    print(f"📊 RESULTADOS:")
    print(f"✅ Permitidos: {permitidos}")
    print(f"❌ Bloqueados: {bloqueados}")
    print(f"⏱️ Tempo total: {tempo_total_ms:.2f} ms")
    print(f"⚡ Média por consulta: {tempo_total_ms/100:.4f} ms")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    testar_massa()