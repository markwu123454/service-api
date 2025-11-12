import json
import base64
import hashlib
from datetime import datetime
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
    yield
    print("Shutting down...")
    await app.state.db_pool.close()


app = FastAPI(lifespan=lifespan)


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
    location: Optional[str] = None
    owner: Optional[str] = None
    notes: Optional[str] = None


# ---------------------------------------------------
# Utility
# ---------------------------------------------------
def compute_hash(code: str) -> str:
    """Compute base64 SHA256 hash of given code string."""
    h = hashlib.sha256(code.encode("utf-8")).digest()
    return base64.b64encode(h).decode()


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
    """Update only last_heartbeat and status for an existing node."""
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE nodes
            SET last_heartbeat = $2,
                status         = 'online'
            WHERE id = $1
            """,
            id,
            datetime.utcnow(),
        )
    if result == "UPDATE 0":
        raise HTTPException(status_code=404, detail="Node ID not found")
    return {"status": "updated", "id": id}


@app.post("/register")
async def register(specs: NodeSpecs, request: Request):
    """Insert or update full device specifications and return the row ID."""
    forbidden = [f for f in ("owner", "notes", "location") if getattr(specs, f)]
    if forbidden:
        raise HTTPException(
            status_code=400,
            detail=f"The following fields must be empty: {', '.join(forbidden)}"
        )

    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        if getattr(specs, "id", None):
            query = """
                    INSERT INTO nodes (id, hostname, ip_address, mac_address, os,
                                       cpu_model, cpu_cores, memory_gb, storage_gb, drives,
                                       gpu_model, location, owner, notes,
                                       status, last_heartbeat, last_checked)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, '', '', '',
                            'online', now(), now())
                    ON CONFLICT (id)
                        DO UPDATE SET hostname=$2,
                                      ip_address=$3,
                                      mac_address=$4,
                                      os=$5,
                                      cpu_model=$6,
                                      cpu_cores=$7,
                                      memory_gb=$8,
                                      storage_gb=$9,
                                      drives=$10,
                                      gpu_model=$11,
                                      location='',
                                      owner='',
                                      notes='',
                                      status='online',
                                      last_heartbeat=now(),
                                      last_checked=now()
                    RETURNING id \
                    """
            row = await conn.fetchrow(
                query,
                specs.id, specs.hostname, specs.ip_address, specs.mac_address,
                specs.os, specs.cpu_model, specs.cpu_cores, specs.memory_gb,
                specs.storage_gb, json.dumps(specs.drives), specs.gpu_model
            )
        else:
            query = """
                    INSERT INTO nodes (hostname, ip_address, mac_address, os,
                                       cpu_model, cpu_cores, memory_gb, storage_gb, drives,
                                       gpu_model, location, owner, notes,
                                       status, last_heartbeat, last_checked)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, '', '', '',
                            'online', now(), now())
                    RETURNING id \
                    """
            row = await conn.fetchrow(
                query,
                specs.hostname, specs.ip_address, specs.mac_address, specs.os,
                specs.cpu_model, specs.cpu_cores, specs.memory_gb, specs.storage_gb,
                json.dumps(specs.drives), specs.gpu_model
            )

    return {"status": "registered", "hostname": specs.hostname, "id": row["id"]}


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
