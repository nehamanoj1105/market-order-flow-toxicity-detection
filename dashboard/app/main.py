from fastapi import FastAPI
from contextlib import asynccontextmanager
import logging
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path
from app.core.config import settings
from app.core.database import connect_db, disconnect_db
from app.core.alert_stream import alert_stream
from app.api.routes import router as api_router

logging.basicConfig(level=logging.INFO)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect_db(settings.PG_DSN)

    await alert_stream.start()

    yield

    await alert_stream.stop()
    await disconnect_db()

frontend_dir = Path(__file__).resolve().parent.parent / "frontend"

app = FastAPI(title=settings.PROJECT_NAME, lifespan=lifespan, docs_url="/docs", redoc_url="/redoc")

app.mount("/static", StaticFiles(directory=frontend_dir),name="static")
app.include_router(api_router, prefix="/api")

@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
    
@app.get("/",include_in_schema=False)
async def dashboard():
    return FileResponse(frontend_dir / "index.html")

