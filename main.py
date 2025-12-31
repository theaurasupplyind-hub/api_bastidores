import os
import datetime
from typing import List, Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends, status, BackgroundTasks, Query
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Boolean, func, select, desc
from sqlalchemy.orm import sessionmaker, Session, declarative_base, relationship, joinedload

# --- CONFIGURACIÓN BASE DE DATOS ---
DATABASE_URL = os.getenv("DATABASE_URL")

# Corrección automática de URL para Render (postgres -> postgresql)
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
# Fallback para pruebas locales si no hay variable de entorno
if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./local_test.db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- MODELOS SQLALCHEMY (TABLAS) ---

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    full_name = Column(String)
    last_seen = Column(DateTime, default=datetime.datetime.utcnow)

class Client(Base):
    __tablename__ = "clientes"
    id = Column(Integer, primary_key=True, index=True)
    nombre = Column(String, nullable=False)
    domicilio = Column(String, nullable=True)
    telefono = Column(String, nullable=True)
    taller = Column(String, nullable=True)
    estudiante = Column(String, nullable=True)

class Product(Base):
    __tablename__ = "productos"
    id = Column(String, primary_key=True) # ID Manual ej: MAN_123
    descripcion = Column(String, nullable=False)
    precio_unitario = Column(Float, nullable=False)
    categoria = Column(String, default="Varios")
    medida = Column(String, default="")
    variante = Column(String, default="")
    precio_lista = Column(Float, default=0)

class Invoice(Base):
    __tablename__ = "facturas"
    id = Column(Integer, primary_key=True, index=True)
    numero_presupuesto = Column(String, nullable=True)
    numero_factura = Column(String, index=True)
    fecha = Column(String)
    cliente_id = Column(Integer, ForeignKey("clientes.id"), nullable=True)
    cliente_nombre = Column(String)
    cliente_domicilio = Column(String, nullable=True)
    cliente_telefono = Column(String, nullable=True)
    total = Column(Float)
    envio = Column(Float, default=0)
    tipo = Column(String, default='PRESUPUESTO')
    estado_orden_tela = Column(String, default='PENDING')
    estado_moldura = Column(String, default='PENDING')
    
    # Auditoría
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    # Relación con items (cascade para borrar items si se borra factura)
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
    user_id = Column(Integer)
    acquired_at = Column(DateTime, default=datetime.datetime.utcnow)

class DraftStatus(Base):
    __tablename__ = "drafts_in_progress"
    user_id = Column(Integer, primary_key=True)
    client_name = Column(String)
    started_at = Column(DateTime, default=datetime.datetime.utcnow)

# --- ESQUEMAS PYDANTIC (VALIDACIÓN DATOS) ---

class HeartbeatRequest(BaseModel):
    user_id: int

class LockRequest(BaseModel):
    user_id: int

class DraftRequest(BaseModel):
    user_id: int
    client_name: str

class ClientCreate(BaseModel):
    nombre: str
    domicilio: Optional[str] = ""
    telefono: Optional[str] = ""
    taller: Optional[str] = ""
    estudiante: Optional[str] = ""

class ProductCreate(BaseModel):
    id: str
    descripcion: str
    precio_unitario: float
    categoria: Optional[str] = "Varios"
    medida: Optional[str] = ""
    variante: Optional[str] = ""
    precio_lista: Optional[float] = 0

class InvoiceItemCreate(BaseModel):
    cantidad: float
    descripcion: str
    precio_unitario: float
    total: float

class InvoiceCreate(BaseModel):
    numero_factura: str
    numero_presupuesto: Optional[str] = ""
    fecha: str
    cliente_id: Optional[int]
    cliente_nombre: str
    cliente_domicilio: Optional[str] = ""
    cliente_telefono: Optional[str] = ""
    items: List[InvoiceItemCreate]
    total: float
    envio: float
    tipo: str = "PRESUPUESTO"
    user_id: int

# --- DEPENDENCIAS ---

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- TAREAS EN SEGUNDO PLANO ---

def cleanup_inactive_users_logic(db: Session):
    limit = datetime.datetime.utcnow() - datetime.timedelta(seconds=30)
    db.query(InvoiceLock).filter(InvoiceLock.acquired_at < limit).delete()
    db.query(DraftStatus).filter(DraftStatus.started_at < limit).delete()
    db.query(User).filter(User.last_seen < limit).delete()
    db.commit()

@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield

# --- APP ---

app = FastAPI(lifespan=lifespan, title="FacBal API")

# 1. HEARTBEAT Y USUARIOS
@app.post("/heartbeat")
def heartbeat(hb: HeartbeatRequest, bg_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == hb.user_id).first()
    if not user:
        user = User(id=hb.user_id, full_name=f"User {hb.user_id}")
        db.add(user)
    user.last_seen = datetime.datetime.utcnow()
    db.commit()
    bg_tasks.add_task(cleanup_inactive_users_logic, db)
    return {"status": "ok"}

@app.get("/users/active")
def get_active_users(db: Session = Depends(get_db)):
    limit = datetime.datetime.utcnow() - datetime.timedelta(seconds=20)
    users = db.query(User).filter(User.last_seen > limit).all()
    return [{"id": u.id, "name": u.full_name} for u in users]

# 2. CLIENTES
@app.get("/clients")
def get_clients(db: Session = Depends(get_db)):
    return db.query(Client).order_by(Client.nombre).all()

@app.get("/clients/{cid}")
def get_client(cid: int, db: Session = Depends(get_db)):
    return db.query(Client).filter(Client.id == cid).first()

@app.post("/clients")
def create_client(client: ClientCreate, db: Session = Depends(get_db)):
    db_client = Client(**client.dict())
    db.add(db_client)
    db.commit()
    db.refresh(db_client)
    return db_client

@app.put("/clients/{cid}")
def update_client(cid: int, client: ClientCreate, db: Session = Depends(get_db)):
    db_c = db.query(Client).filter(Client.id == cid).first()
    if db_c:
        for key, value in client.dict().items():
            setattr(db_c, key, value)
        db.commit()
    return {"status": "ok"}

@app.delete("/clients/{cid}")
def delete_client(cid: int, db: Session = Depends(get_db)):
    db.query(Client).filter(Client.id == cid).delete()
    db.commit()
    return {"status": "deleted"}

# 3. PRODUCTOS
@app.get("/products")
def get_products(db: Session = Depends(get_db)):
    return db.query(Product).order_by(Product.categoria, Product.descripcion).all()

@app.post("/products")
def create_product(prod: ProductCreate, db: Session = Depends(get_db)):
    existing = db.query(Product).filter(Product.id == prod.id).first()
    if existing:
        for key, value in prod.dict().items():
            setattr(existing, key, value)
    else:
        new_prod = Product(**prod.dict())
        db.add(new_prod)
    db.commit()
    return {"status": "ok"}

@app.put("/products/{pid}")
def update_product(pid: str, prod: ProductCreate, db: Session = Depends(get_db)):
    existing = db.query(Product).filter(Product.id == pid).first()
    if existing:
        for key, value in prod.dict().items():
            setattr(existing, key, value)
        db.commit()
    return {"status": "ok"}

@app.delete("/products/{pid}")
def delete_product(pid: str, db: Session = Depends(get_db)):
    db.query(Product).filter(Product.id == pid).delete()
    db.commit()
    return {"status": "deleted"}

# 4. FACTURAS
@app.get("/invoices")
def get_invoices(search: Optional[str] = None, limit: int = 50, db: Session = Depends(get_db)):
    # NOTA: Este endpoint es para listas (historial). No trae items para ser rápido.
    q = db.query(Invoice)
    if search:
        s = f"%{search}%"
        q = q.filter((Invoice.cliente_nombre.ilike(s)) | (Invoice.numero_factura.ilike(s)))
    return q.order_by(desc(Invoice.id)).limit(limit).all()

@app.get("/invoices/{fid}")
def get_invoice(fid: int, db: Session = Depends(get_db)):
    # --- CORRECCIÓN CRÍTICA: joinedload para traer items ---
    inv = db.query(Invoice).options(joinedload(Invoice.items)).filter(Invoice.id == fid).first()
    if not inv: raise HTTPException(404, "Factura no encontrada")
    return inv

@app.post("/invoices")
def create_invoice(inv: InvoiceCreate, db: Session = Depends(get_db)):
    # Crear cabecera
    db_inv = Invoice(
        numero_factura=inv.numero_factura,
        numero_presupuesto=inv.numero_presupuesto,
        fecha=inv.fecha,
        cliente_id=inv.cliente_id,
        cliente_nombre=inv.cliente_nombre,
        cliente_domicilio=inv.cliente_domicilio,
        cliente_telefono=inv.cliente_telefono,
        total=inv.total,
        envio=inv.envio,
        tipo=inv.tipo,
        created_by=inv.user_id
    )
    db.add(db_inv)
    db.flush() # Genera el ID
    
    # Crear items
    for item in inv.items:
        db_item = InvoiceItem(
            factura_id=db_inv.id,
            cantidad=item.cantidad,
            descripcion=item.descripcion,
            precio_unitario=item.precio_unitario,
            total=item.total
        )
        db.add(db_item)
    
    db.query(DraftStatus).filter(DraftStatus.user_id == inv.user_id).delete()
    
    db.commit()
    db.refresh(db_inv)
    return db_inv

@app.put("/invoices/{fid}")
def update_invoice(fid: int, inv: InvoiceCreate, db: Session = Depends(get_db)):
    db_inv = db.query(Invoice).filter(Invoice.id == fid).first()
    if not db_inv: raise HTTPException(404)
    
    db_inv.numero_factura = inv.numero_factura
    db_inv.fecha = inv.fecha
    db_inv.cliente_id = inv.cliente_id
    db_inv.cliente_nombre = inv.cliente_nombre
    db_inv.cliente_domicilio = inv.cliente_domicilio
    db_inv.cliente_telefono = inv.cliente_telefono
    db_inv.total = inv.total
    db_inv.envio = inv.envio
    
    db.query(InvoiceItem).filter(InvoiceItem.factura_id == fid).delete()
    for item in inv.items:
        db_item = InvoiceItem(
            factura_id=fid,
            cantidad=item.cantidad,
            descripcion=item.descripcion,
            precio_unitario=item.precio_unitario,
            total=item.total
        )
        db.add(db_item)
        
    db.commit()
    return {"status": "updated"}

@app.delete("/invoices/{fid}")
def delete_invoice(fid: int, db: Session = Depends(get_db)):
    db.query(Invoice).filter(Invoice.id == fid).delete()
    db.commit()
    return {"status": "deleted"}

@app.get("/invoices/next_number")
def next_number(prefix: str = "F", db: Session = Depends(get_db)):
    last = db.query(Invoice).filter(Invoice.numero_factura.like(f"{prefix}-%")).order_by(desc(Invoice.id)).first()
    if last:
        try:
            num = int(last.numero_factura.split("-")[1])
            return {"next_number": f"{prefix}-{str(num+1).zfill(5)}"}
        except: pass
    return {"next_number": f"{prefix}-00001"}

# 5. LOCKS & DRAFTS
@app.post("/invoices/{fid}/lock")
def acquire_lock(fid: int, req: LockRequest, db: Session = Depends(get_db)):
    existing = db.query(InvoiceLock).filter(InvoiceLock.invoice_id == fid).first()
    if existing:
        if existing.user_id != req.user_id:
            raise HTTPException(409, "Factura bloqueada por otro usuario")
        existing.acquired_at = datetime.datetime.utcnow()
    else:
        db.add(InvoiceLock(invoice_id=fid, user_id=req.user_id))
    db.commit()
    return {"status": "locked"}

@app.post("/invoices/draft")
def register_draft(req: DraftRequest, db: Session = Depends(get_db)):
    draft = db.query(DraftStatus).filter(DraftStatus.user_id == req.user_id).first()
    if not draft:
        draft = DraftStatus(user_id=req.user_id)
        db.add(draft)
    draft.client_name = req.client_name
    draft.started_at = datetime.datetime.utcnow()
    db.commit()
    return {"status": "ok"}

@app.get("/invoices/drafts")
def get_drafts(db: Session = Depends(get_db)):
    drafts = db.query(DraftStatus).all()
    return [{"user": f"User {d.user_id}", "client": d.client_name, "user_id": d.user_id} for d in drafts]