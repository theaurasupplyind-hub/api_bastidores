import os
import datetime
from typing import List, Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends, status, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Boolean, func, select, delete, and_
from sqlalchemy.orm import sessionmaker, Session, declarative_base, relationship
from sqlalchemy.exc import IntegrityError

# --- CONFIGURATION ---
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/facbal_db")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- MODELS (DB SCHEMA) ---

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    full_name = Column(String)
    last_seen = Column(DateTime, default=datetime.datetime.utcnow)

class Client(Base):
    __tablename__ = "clientes"
    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String, nullable=False)
    domicilio = Column(String)
    telefono = Column(String)
    taller = Column(String)
    estudiante = Column(String)

class Product(Base):
    __tablename__ = "productos"
    id = Column(String, primary_key=True) # Manual ID like MAN_123
    descripcion = Column(String, nullable=False)
    precio_unitario = Column(Float, nullable=False)
    categoria = Column(String)
    medida = Column(String)
    variante = Column(String)
    precio_lista = Column(Float, default=0)

class Invoice(Base):
    __tablename__ = "facturas"
    id = Column(Integer, primary_key=True, index=True)
    numero_presupuesto = Column(String)
    numero_factura = Column(String, unique=True, index=True)
    fecha = Column(String) # Keeping string for legacy compat, ideally Date
    cliente_id = Column(Integer, ForeignKey("clientes.id"), nullable=True)
    cliente_nombre = Column(String)
    cliente_domicilio = Column(String)
    cliente_telefono = Column(String)
    total = Column(Float)
    envio = Column(Float, default=0)
    tipo = Column(String, default='PRESUPUESTO')
    estado_orden_tela = Column(String, default='PENDING')
    estado_moldura = Column(String, default='PENDING')
    
    # Audit Fields
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    updated_at = Column(DateTime, onupdate=datetime.datetime.utcnow)

    items = relationship("InvoiceItem", back_populates="invoice", cascade="all, delete-orphan")

class InvoiceItem(Base):
    __tablename__ = "items_factura"
    id = Column(Integer, primary_key=True, index=True)
    factura_id = Column(Integer, ForeignKey("facturas.id"))
    cantidad = Column(Float)
    descripcion = Column(String)
    precio_unitario = Column(Float)
    total = Column(Float)
    invoice = relationship("Invoice", back_populates="items")

class InvoiceLock(Base):
    __tablename__ = "invoice_locks"
    invoice_id = Column(Integer, ForeignKey("facturas.id"), primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    acquired_at = Column(DateTime, default=datetime.datetime.utcnow)
    user = relationship("User")

class DraftStatus(Base):
    __tablename__ = "drafts_in_progress"
    user_id = Column(Integer, ForeignKey("users.id"), primary_key=True)
    client_name = Column(String)
    started_at = Column(DateTime, default=datetime.datetime.utcnow)
    user = relationship("User")

# --- PYDANTIC SCHEMAS ---

class HeartbeatRequest(BaseModel):
    user_id: int

class LockRequest(BaseModel):
    user_id: int

class DraftRequest(BaseModel):
    user_id: int
    client_name: str

class InvoiceCreate(BaseModel):
    numero_factura: str
    fecha: str
    cliente_id: Optional[int]
    cliente_nombre: str
    cliente_domicilio: str
    cliente_telefono: str
    items: List[dict]
    total: float
    envio: float
    user_id: int

# --- DEPENDENCIES ---

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- BACKGROUND TASKS ---

def cleanup_inactive_users_logic(db: Session):
    # Threshold: 30 seconds inactivity
    limit = datetime.datetime.utcnow() - datetime.timedelta(seconds=30)
    
    # 1. Find inactive users
    inactive_users = db.query(User.id).filter(User.last_seen < limit).all()
    ids = [u.id for u in inactive_users]
    
    if ids:
        # 2. Release Locks
        db.query(InvoiceLock).filter(InvoiceLock.user_id.in_(ids)).delete(synchronize_session=False)
        # 3. Clear Drafts
        db.query(DraftStatus).filter(DraftStatus.user_id.in_(ids)).delete(synchronize_session=False)
        db.commit()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Create tables
    Base.metadata.create_all(bind=engine)
    yield

# --- API ENDPOINTS ---

app = FastAPI(lifespan=lifespan, title="FacBal API")

@app.post("/heartbeat")
def heartbeat(hb: HeartbeatRequest, bg_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == hb.user_id).first()
    if user:
        user.last_seen = datetime.datetime.utcnow()
        db.commit()
        # Schedule cleanup
        bg_tasks.add_task(cleanup_inactive_users_logic, db)
        return {"status": "ok", "online": True}
    raise HTTPException(status_code=404, detail="User not found")

@app.get("/users/active")
def get_active_users(db: Session = Depends(get_db)):
    # Users active in last 30s
    limit = datetime.datetime.utcnow() - datetime.timedelta(seconds=30)
    users = db.query(User).filter(User.last_seen > limit).all()
    return [{"id": u.id, "name": u.full_name} for u in users]

@app.post("/invoices/{invoice_id}/lock")
def acquire_lock(invoice_id: int, req: LockRequest, db: Session = Depends(get_db)):
    existing = db.query(InvoiceLock).filter(InvoiceLock.invoice_id == invoice_id).first()
    if existing:
        if existing.user_id == req.user_id:
            # Refresh lock
            existing.acquired_at = datetime.datetime.utcnow()
            db.commit()
            return {"status": "refreshed"}
        else:
            # Locked by someone else
            locker = db.query(User).filter(User.id == existing.user_id).first()
            name = locker.full_name if locker else "Unknown"
            raise HTTPException(status_code=409, detail=f"Locked by {name}")
    
    # Create Lock
    new_lock = InvoiceLock(invoice_id=invoice_id, user_id=req.user_id)
    db.add(new_lock)
    db.commit()
    return {"status": "acquired"}

@app.delete("/invoices/{invoice_id}/lock")
def release_lock(invoice_id: int, req: LockRequest, db: Session = Depends(get_db)):
    db.query(InvoiceLock).filter(
        InvoiceLock.invoice_id == invoice_id, 
        InvoiceLock.user_id == req.user_id
    ).delete()
    db.commit()
    return {"status": "released"}

@app.post("/invoices/draft")
def register_draft(req: DraftRequest, db: Session = Depends(get_db)):
    # Upsert draft
    draft = db.query(DraftStatus).filter(DraftStatus.user_id == req.user_id).first()
    if not draft:
        draft = DraftStatus(user_id=req.user_id)
        db.add(draft)
    
    draft.client_name = req.client_name
    draft.started_at = datetime.datetime.utcnow()
    db.commit()
    return {"status": "registered"}

@app.get("/invoices/drafts")
def get_all_drafts(db: Session = Depends(get_db)):
    drafts = db.query(DraftStatus).all()
    return [
        {"user": d.user.full_name, "client": d.client_name, "since": d.started_at} 
        for d in drafts if d.user
    ]

@app.post("/invoices")
def create_invoice(inv: InvoiceCreate, db: Session = Depends(get_db)):
    # 1. Validation or Numbering Logic could go here
    # 2. Check duplicate number
    if db.query(Invoice).filter(Invoice.numero_factura == inv.numero_factura).first():
         # Simple auto-fix logic or error
         raise HTTPException(status_code=400, detail="Invoice number exists")

    db_inv = Invoice(
        numero_factura=inv.numero_factura,
        fecha=inv.fecha,
        cliente_id=inv.cliente_id,
        cliente_nombre=inv.cliente_nombre,
        cliente_domicilio=inv.cliente_domicilio,
        cliente_telefono=inv.cliente_telefono,
        total=inv.total,
        envio=inv.envio,
        created_by=inv.user_id
    )
    db.add(db_inv)
    db.flush() # get ID

    for item in inv.items:
        db_item = InvoiceItem(
            factura_id=db_inv.id,
            cantidad=item['cantidad'],
            descripcion=item['descripcion'],
            precio_unitario=item['precio_unitario'],
            total=item['total']
        )
        db.add(db_item)
    
    # Remove draft status for this user
    db.query(DraftStatus).filter(DraftStatus.user_id == inv.user_id).delete()
    
    db.commit()
    return {"id": db_inv.id, "status": "created"}