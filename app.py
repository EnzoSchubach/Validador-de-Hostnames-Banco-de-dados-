import psycopg2 # Certifique-se que este import está no topo do app.py
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import os

from DAL.whitelist_db import WhitelistDB
from cache_manager import cache_duo
from workers import executar_probe_assincrono

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

db = WhitelistDB()

@app.get("/")
async def read_index():
    return FileResponse('templates/index.html')

@app.post("/api/v1/check")
async def check_hostnames(hostnames: list[str], background_tasks: BackgroundTasks):
    respostas = []
    for host in hostnames:
        hit = cache_duo.obter(host)
        if hit:
            respostas.append({"hostname": host, "status": hit, "reason": "Cache L1"})
            continue

        res_db = db.check_hostname(host)
        
        if res_db["allowed"]:
            background_tasks.add_task(executar_probe_assincrono, host)
            status = "VALID"
        else:
            status = "DISALLOWED"
            
        respostas.append({
            "hostname": host,
            "status": status,
            "reason": res_db["reason"]
        })
    return respostas



@app.get("/api/v1/history")
async def get_history():
    conn = None
    try:
        # Conexão idêntica ao que você testou no terminal agora
        conn = psycopg2.connect(
            dbname="validador_hostnames",
            user="postgres",
            password="postgres", # <--- COLOQUE A SENHA AQUI
            host="localhost",
            port="5432"
        )
        
        with conn.cursor() as cur:
            cur.execute("""
                SELECT hostname, status, latency_ms, resolved_ips, checked_at 
                FROM probe_history 
                ORDER BY checked_at DESC 
                LIMIT 10
            """)
            rows = cur.fetchall()
        
        history = []
        for row in rows:
            history.append({
                "hostname": row[0],
                "status": row[1],
                "latency_ms": row[2],
                "resolved_ips": row[3],
                "checked_at": row[4].isoformat() if row[4] else None
            })
        return history
    except Exception as e:
        print(f"Erro ao buscar histórico: {e}")
        return []
    finally:
        if conn:
            conn.close()