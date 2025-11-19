import asyncio
import json
import base64
import hashlib
from datetime import datetime, timedelta
import os
from typing import Dict, Optional
import asyncpg
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from pydantic import BaseModel
from pathlib import Path

# ---------------------------------------------------
# Setup
# ---------------------------------------------------
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

BASE_DIR = Path(__file__).parent
PUBLIC_KEY_PATH = BASE_DIR / "public.pem"

VERSION_FALLBACK = "0.0.0"  # used if DB empty


# ---------------------------------------------------
# Database connection
# ---------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting up...")
    app.state.db_pool = await asyncpg.create_pool(DB_URL)

    # -----------------------------
    # BACKGROUND OFFLINE SCANNER
    # -----------------------------
    async def offline_scanner():
        while True:
            try:
                async with app.state.db_pool.acquire() as conn:
                    rows = await conn.fetch("""
                                            SELECT id, status
                                            FROM nodes
                                            WHERE last_heartbeat < (now() - interval '70 seconds')
                                              AND status != 'offline'
                                            """)

                    for r in rows:
                        node_id = r["id"]
                        old_status = r["status"]

                        # Perform update
                        await conn.execute("""
                                           UPDATE nodes
                                           SET status='offline',
                                               last_checked=now()
                                           WHERE id = $1
                                           """, node_id)

                        # Log transition
                        await log_field_change(conn, node_id, "status", old_status, "offline")

            except Exception as e:
                print("offline_scanner error:", e)

            await asyncio.sleep(15)

    # start task
    app.state.scanner_task = asyncio.create_task(offline_scanner())

    yield

    print("Shutting down...")
    app.state.scanner_task.cancel()
    await app.state.db_pool.close()



app = FastAPI(
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None
)


# ---------------------------------------------------
# Models
# ---------------------------------------------------
class NodeSpecs(BaseModel):
    id: Optional[str] = None
    hostname: str
    ip_address: str
    mac_address: Optional[str] = None
    os: Optional[str] = None
    cpu_model: Optional[str] = None
    cpu_cores: Optional[int] = None
    memory_gb: Optional[float] = None
    storage_gb: Optional[float] = None
    drives: Optional[Dict[str, float]] = None
    gpu_model: Optional[str] = None
    version: Optional[int] = None


# ---------------------------------------------------
# Utility
# ---------------------------------------------------
def compute_hash(code: str) -> str:
    """Compute base64 SHA256 hash of given code string."""
    h = hashlib.sha256(code.encode("utf-8")).digest()
    return base64.b64encode(h).decode()


async def log_field_change(conn, node_id, field, old, new):
    if old == new:
        return
    await conn.execute("""
                       INSERT INTO node_change_log (node_id, field_name, old_value, new_value, changed_at)
                       VALUES ($1, $2, $3, $4, now())
                       """, node_id, field, str(old) if old is not None else None,
                       str(new) if new is not None else None)


# ---------------------------------------------------
# Endpoints
# ---------------------------------------------------
@app.get("/")
async def root():
    return {"status": "ok"}


@app.get("/ping")
async def ping():
    return {"ping": "pong"}


@app.post("/heartbeat")
async def heartbeat(request: Request, id: str):
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:

        old_row = await conn.fetchrow("""
            SELECT status
            FROM nodes
            WHERE id=$1
        """, id)

        if not old_row:
            raise HTTPException(status_code=404, detail="Node ID not found")

        old_status = old_row["status"]

        await conn.execute("""
            UPDATE nodes
            SET last_heartbeat = now(),
                status = 'online'
            WHERE id = $1
        """, id)

        if old_status != "online":
            await log_field_change(conn, id, "status", old_status, "online")

    return {"status": "updated", "id": id}



@app.post("/register")
async def register(specs: NodeSpecs, request: Request):
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:

        old_row = None
        if getattr(specs, "id", None):
            old_row = await conn.fetchrow("SELECT * FROM nodes WHERE id=$1", specs.id)

        if getattr(specs, "id", None):
            query = """
                INSERT INTO nodes (
                    id, hostname, ip_address, mac_address, os,
                    cpu_model, cpu_cores, memory_gb, storage_gb, drives,
                    gpu_model, version, location, owner, notes,
                    status, last_heartbeat, last_checked
                )
                VALUES (
                    $1, $2, $3, $4, $5,
                    $6, $7, $8, $9, $10,
                    $11, $12, '', '', '',
                    'offline', NULL, NULL
                )
                ON CONFLICT (id)
                DO UPDATE SET
                    hostname      = COALESCE(EXCLUDED.hostname, nodes.hostname),
                    ip_address    = COALESCE(EXCLUDED.ip_address, nodes.ip_address),
                    mac_address   = COALESCE(EXCLUDED.mac_address, nodes.mac_address),
                    os            = COALESCE(EXCLUDED.os, nodes.os),
                    cpu_model     = COALESCE(EXCLUDED.cpu_model, nodes.cpu_model),
                    cpu_cores     = COALESCE(EXCLUDED.cpu_cores, nodes.cpu_cores),
                    memory_gb     = COALESCE(EXCLUDED.memory_gb, nodes.memory_gb),
                    storage_gb    = COALESCE(EXCLUDED.storage_gb, nodes.storage_gb),
                    drives        = COALESCE(EXCLUDED.drives, nodes.drives),
                    gpu_model     = COALESCE(EXCLUDED.gpu_model, nodes.gpu_model),
                    version       = COALESCE(EXCLUDED.version, nodes.version)
                RETURNING *
            """
            row = await conn.fetchrow(
                query,
                specs.id,
                specs.hostname,
                specs.ip_address,
                specs.mac_address,
                specs.os,
                specs.cpu_model,
                specs.cpu_cores,
                specs.memory_gb,
                specs.storage_gb,
                json.dumps(specs.drives) if specs.drives else None,
                specs.gpu_model,
                specs.version
            )

        else:
            query = """
                INSERT INTO nodes (
                    hostname, ip_address, mac_address, os,
                    cpu_model, cpu_cores, memory_gb, storage_gb, drives,
                    gpu_model, version, location, owner, notes,
                    status, last_heartbeat, last_checked
                )
                VALUES (
                    $1, $2, $3, $4,
                    $5, $6, $7, $8, $9,
                    $10, $11, '', '', '',
                    'offline', NULL, NULL
                )
                RETURNING *
            """
            row = await conn.fetchrow(
                query,
                specs.hostname,
                specs.ip_address,
                specs.mac_address,
                specs.os,
                specs.cpu_model,
                specs.cpu_cores,
                specs.memory_gb,
                specs.storage_gb,
                json.dumps(specs.drives) if specs.drives else None,
                specs.gpu_model,
                specs.version
            )

        # no status logging
        if old_row:
            fields = [
                "hostname", "ip_address", "mac_address", "os",
                "cpu_model", "cpu_cores", "memory_gb", "storage_gb",
                "drives", "gpu_model", "version"
            ]
            for f in fields:
                old = old_row[f]
                new = row[f]
                await log_field_change(conn, row["id"], f, old, new)

    return {"status": "registered", "hostname": row["hostname"], "id": row["id"]}



@app.post("/logoff")
async def logoff(id: str, request: Request):
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:

        # fetch current status
        old = await conn.fetchrow("""
                                  SELECT status
                                  FROM nodes
                                  WHERE id = $1
                                  """, id)

        if not old:
            raise HTTPException(status_code=404, detail="Node ID not found")

        old_status = old["status"]

        # only update + log if status actually changes
        if old_status != "offline":
            await conn.execute("""
                               UPDATE nodes
                               SET status       = 'offline',
                                   last_checked = now()
                               WHERE id = $1
                               """, id)

            # log only real transitions
            await log_field_change(
                conn, id,
                "status",
                old_status,
                "offline"
            )

    return {"status": "offline", "id": id}


# ---------------------------------------------------
# Update endpoints
# ---------------------------------------------------
@app.get("/update")
async def get_update(request: Request, hash: Optional[str] = Query(None)):
    """
    Return signed payload only if there is a newer version.
    Request param: ?hash=<local_base64_hash>
    """
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT version, code, signature, hash
               FROM payloads
               ORDER BY version DESC
               LIMIT 1"""
        )
        if not row:
            raise HTTPException(status_code=503, detail="No payloads available yet")

        version = row["version"]
        code = row["code"]
        signature = row["signature"]
        stored_hash = row["hash"]

        if hash and hash == stored_hash:
            # client already has latest
            return {}

        return JSONResponse({
            "version": str(version),
            "signature": signature,
            "hash": stored_hash,
            "code": code,
        })


@app.get("/public_key")
async def get_public_key():
    """Serve the public RSA key (hard-coded for integrity)."""
    return JSONResponse({
        "public_key": """-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA8sPfrF4g6PreceYsXlFm
8EfXnbrVw357XIONBw944ltP708tnM4bRUBHlVoKdHigmoTjWgiz3IsKozOWsydp
qcQWB/ooQKqi4/Quvy1H5f2MFdlLZnyFPZOsW4Cq6X7ngtyxM0+WpDkU4EMBgwuL
Zwvqx1UYeh0prRqEsR4x766NjYDklaSf6Xbj4GQRrYkWRi3Up47cRD3GIH33AhbJ
AwlxqefLHeicMAT5s+povQGjnLizKqNgLFjIzaMfKbAifQ6jfvq/pG7WWcUSkZ4c
p1IdGVbcH50OtxwKervSH1QDhF4Es2O9gHK/+admpSTko7fK7wHc5fk/anH2Hzzl
nwIDAQAB
-----END PUBLIC KEY-----"""
    })
