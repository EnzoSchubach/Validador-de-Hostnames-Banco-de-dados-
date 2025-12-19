import dns.resolver
import httpx
import time
import asyncio
from cache_manager import cache_duo
from DAL.whitelist_db import WhitelistDB

# Instância para salvar o histórico no Postgres
db_dal = WhitelistDB()

async def executar_probe_assincrono(hostname: str):
    """Executa o probe e salva o resultado final no banco e no cache."""
    start_time = time.perf_counter()
    ips = []
    status_final = "UNKNOWN"
    
    try:
        # 1. Probe de DNS
        try:
            answers = dns.resolver.resolve(hostname, 'A')
            ips = [str(rdata) for rdata in answers]
        except:
            ips = []
        
        # 2. Probe HTTP
        async with httpx.AsyncClient(timeout=3.0) as client:
            response = await client.get(f"http://{hostname}")
            status_final = "VALID" if response.status_code < 400 else "DISALLOWED"
            
    except Exception:
        status_final = "UNKNOWN"
    
    latency = round((time.perf_counter() - start_time) * 1000, 2)

    cache_duo.atualizar(hostname, status_final)
    
    # Salva no Histórico convertendo a lista para o formato que o Postgres aceita
    try:
        db_dal.save_probe_history(
            hostname=hostname, 
            status=status_final, 
            latency=latency, 
            ips=ips, # O driver psycopg2 converte lista python para ARRAY sql automaticamente
            reason="Probe de rede concluído"
        )
    except Exception as e:
        print(f"❌ Erro ao salvar histórico: {e}")
        
    print(f"DEBUG: Probe finalizado para {hostname} -> {status_final}")