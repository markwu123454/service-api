from datetime import datetime
import os
import asyncpg
from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting up...")

    # Initialize connection pool
    app.state.db_pool = await asyncpg.create_pool(DB_URL)

    yield

    print("Shutting down...")
    await app.state.db_pool.close()

app = FastAPI(lifespan=lifespan)



class NodeSpecs(BaseModel):
    hostname: str
    ip_address: str
    mac_address: str | None = None
    os: str | None = None
    cpu_model: str | None = None
    cpu_cores: int | None = None
    memory_gb: float | None = None
    storage_gb: float | None = None
    gpu_model: str | None = None
    location: str | None = None
    owner: str | None = None
    notes: str | None = None




@app.get("/")
async def root():
    return "hello world"

@app.get("/ping")
async def ping():
    return {"ping": "pong"}

@app.post("/heartbeat")
async def heartbeat(request: Request, hostname: str):
    """Update only last_heartbeat and status for an existing node."""
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE nodes
            SET last_heartbeat = $2,
                status = 'online'
            WHERE hostname = $1
            """,
            hostname,
            datetime.utcnow(),
        )
    return {"status": "updated", "hostname": hostname}


@app.post("/register")
async def register(specs: NodeSpecs, request: Request):
    """Insert or update full device specifications."""
    pool = request.app.state.db_pool
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO nodes (
                hostname, ip_address, mac_address, os,
                cpu_model, cpu_cores, memory_gb, storage_gb,
                gpu_model, location, owner, notes,
                status, last_heartbeat, last_checked
            ) VALUES (
                $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
                'online', now(), now()
            )
            ON CONFLICT (hostname)
            DO UPDATE SET
                ip_address = EXCLUDED.ip_address,
                mac_address = EXCLUDED.mac_address,
                os = EXCLUDED.os,
                cpu_model = EXCLUDED.cpu_model,
                cpu_cores = EXCLUDED.cpu_cores,
                memory_gb = EXCLUDED.memory_gb,
                storage_gb = EXCLUDED.storage_gb,
                gpu_model = EXCLUDED.gpu_model,
                location = EXCLUDED.location,
                owner = EXCLUDED.owner,
                notes = EXCLUDED.notes,
                status = 'online',
                last_heartbeat = now(),
                last_checked = now()
            """,
            specs.hostname, specs.ip_address, specs.mac_address, specs.os,
            specs.cpu_model, specs.cpu_cores, specs.memory_gb, specs.storage_gb,
            specs.gpu_model, specs.location, specs.owner, specs.notes,
        )
    return {"status": "registered", "hostname": specs.hostname}


@app.post("/online")
async def online():
    return {"ping": "pong"}