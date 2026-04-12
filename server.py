from fastapi import FastAPI, APIRouter, HTTPException, Depends, UploadFile, File
from fastapi.responses import FileResponse, StreamingResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional
from datetime import datetime, timezone
from passlib.context import CryptContext
import jwt
from io import BytesIO
import tempfile
import json

# Import report generators
from reports import generate_trial_balance_pdf, generate_trial_balance_excel, generate_trial_balance_word
from reports import generate_account_statement_pdf, generate_account_statement_excel, generate_account_statement_word
from reports import generate_general_ledger_pdf, generate_general_ledger_excel, generate_general_ledger_word
from quarterly_reports_export import generate_quarterly_report_pdf, generate_quarterly_report_pptx

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# Load default chart of accounts from JSON file
def load_default_chart_of_accounts():
    """Load the default chart of accounts from JSON file"""
    json_path = ROOT_DIR / 'data' / 'default_chart_of_accounts.json'
    if json_path.exists():
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []

DEFAULT_CHART_OF_ACCOUNTS = load_default_chart_of_accounts()

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# Helper function to get tenant-specific database
def get_tenant_db(tenant_id: str = None):
    """Get database for a specific tenant or default database"""
    if tenant_id:
        return client[f"{os.environ['DB_NAME']}_{tenant_id}"]
    return db

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# JWT Secret
JWT_SECRET = os.environ.get('JWT_SECRET', 'your-secret-key-change-in-production')
JWT_ALGORITHM = "HS256"

# Super Admin Credentials (Owner)
SUPER_ADMIN_USERNAME = os.environ.get('SUPER_ADMIN_USERNAME', 'owner')
SUPER_ADMIN_PASSWORD = os.environ.get('SUPER_ADMIN_PASSWORD', 'owner@2024')

# Security: Brute Force Protection
LOGIN_ATTEMPTS = {}  # {ip_or_username: {"count": 0, "blocked_until": None}}
MAX_LOGIN_ATTEMPTS = 5
BLOCK_DURATION_MINUTES = 15

def check_brute_force(identifier: str) -> bool:
    """Check if login is blocked due to too many failed attempts"""
    if identifier in LOGIN_ATTEMPTS:
        attempt_info = LOGIN_ATTEMPTS[identifier]
        if attempt_info.get("blocked_until"):
            if datetime.now(timezone.utc) < attempt_info["blocked_until"]:
                return False  # Still blocked
            else:
                # Block expired, reset
                LOGIN_ATTEMPTS[identifier] = {"count": 0, "blocked_until": None}
    return True  # Not blocked

def record_failed_login(identifier: str):
    """Record a failed login attempt"""
    if identifier not in LOGIN_ATTEMPTS:
        LOGIN_ATTEMPTS[identifier] = {"count": 0, "blocked_until": None}
    
    LOGIN_ATTEMPTS[identifier]["count"] += 1
    
    if LOGIN_ATTEMPTS[identifier]["count"] >= MAX_LOGIN_ATTEMPTS:
        from datetime import timedelta
        LOGIN_ATTEMPTS[identifier]["blocked_until"] = datetime.now(timezone.utc) + timedelta(minutes=BLOCK_DURATION_MINUTES)

def reset_login_attempts(identifier: str):
    """Reset login attempts after successful login"""
    if identifier in LOGIN_ATTEMPTS:
        del LOGIN_ATTEMPTS[identifier]

app = FastAPI()
api_router = APIRouter(prefix="/api")

# ==================== Models ====================

# Tenant Model (للنسخ/الشركات)
class Tenant(BaseModel):
    model_config = ConfigDict(extra="ignore")
    tenant_id: str  # معرف فريد للنسخة
    company_name_ar: str
    company_name_en: str
    contact_person: str
    phone: str
    email: str
    license_type: str = "standard"  # standard, premium, enterprise
    license_expiry: str  # تاريخ انتهاء الترخيص
    max_users: int = 5
    is_active: bool = True
    features: List[str] = []  # الميزات المتاحة
    notes: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class TenantCreate(BaseModel):
    company_name_ar: str
    company_name_en: str
    contact_person: str
    phone: str
    email: str
    license_type: str = "standard"
    license_expiry: str
    max_users: int = 5
    features: List[str] = []
    notes: str = ""

class SuperAdminLogin(BaseModel):
    username: str
    password: str

class UserLogin(BaseModel):
    username: str
    password: str

class User(BaseModel):
    model_config = ConfigDict(extra="ignore")
    username: str
    password_hash: str
    full_name: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class LoginRequest(BaseModel):
    username: str
    password: str

class LoginResponse(BaseModel):
    token: str
    username: str
    full_name: str

class ChartOfAccount(BaseModel):
    model_config = ConfigDict(extra="ignore")
    account_code: str
    account_name_ar: str
    account_name_en: str
    account_type: str  # asset, liability, equity, revenue, expense
    parent_code: Optional[str] = None
    level: int
    is_active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class OpeningBalance(BaseModel):
    model_config = ConfigDict(extra="ignore")
    account_code: str
    debit: float = 0.0
    credit: float = 0.0
    fiscal_year: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class Customer(BaseModel):
    model_config = ConfigDict(extra="ignore")
    customer_code: str
    customer_name: str
    tax_number: Optional[str] = None  # VAT Number
    phone: Optional[str] = None
    email: Optional[str] = None
    address: Optional[str] = None
    is_active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class InvoiceItem(BaseModel):
    description: str
    quantity: float
    unit_price: float
    total: float

class Invoice(BaseModel):
    model_config = ConfigDict(extra="ignore")
    invoice_number: str
    invoice_date: datetime
    customer_code: str
    items: List[InvoiceItem]
    subtotal: float
    vat_rate: float = 15.0  # Saudi VAT rate
    vat_amount: float
    total_amount: float
    notes: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class JournalEntryLine(BaseModel):
    account_code: str
    description: str
    debit: float = 0.0
    credit: float = 0.0
    cost_center_code: Optional[str] = None
    project_code: Optional[str] = None

class JournalEntry(BaseModel):
    model_config = ConfigDict(extra="ignore")
    entry_number: str
    entry_date: datetime
    description: str
    lines: List[JournalEntryLine]
    total_debit: float
    total_credit: float
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class ReceiptVoucher(BaseModel):
    model_config = ConfigDict(extra="ignore")
    voucher_number: str
    voucher_date: datetime
    customer_code: Optional[str] = None
    received_from: str
    amount: float
    payment_method: str  # cash, check, bank_transfer
    account_code: str  # The account receiving the money
    description: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class PaymentVoucher(BaseModel):
    model_config = ConfigDict(extra="ignore")
    voucher_number: str
    voucher_date: datetime
    paid_to: str
    amount: float
    payment_method: str  # cash, check, bank_transfer
    account_code: str  # The account paying from
    description: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

# ==================== Cost Centers & Projects Models ====================

class CostCenter(BaseModel):
    model_config = ConfigDict(extra="ignore")
    center_code: str
    center_name_ar: str
    center_name_en: str = ""
    parent_code: Optional[str] = None
    is_active: bool = True
    description: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class Project(BaseModel):
    model_config = ConfigDict(extra="ignore")
    project_code: str
    project_name_ar: str
    project_name_en: str = ""
    client_name: str = ""
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    budget: float = 0.0
    status: str = "active"  # active, completed, on_hold, cancelled
    is_active: bool = True
    description: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

# ==================== Authentication ====================

def verify_token(token: str):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except:
        raise HTTPException(status_code=401, detail="Invalid token")

@api_router.post("/auth/login", response_model=LoginResponse)
async def login(request: LoginRequest):
    user = await db.users.find_one({"username": request.username}, {"_id": 0})
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    if not pwd_context.verify(request.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    token = jwt.encode({"username": user["username"]}, JWT_SECRET, algorithm=JWT_ALGORITHM)
    
    return LoginResponse(
        token=token,
        username=user["username"],
        full_name=user["full_name"]
    )

@api_router.post("/auth/init")
async def init_user():
    """Initialize default admin user"""
    existing = await db.users.find_one({"username": "admin"})
    if existing:
        return {"message": "User already exists"}
    
    user = User(
        username="admin",
        password_hash=pwd_context.hash("admin123"),
        full_name="Administrator"
    )
    
    doc = user.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    await db.users.insert_one(doc)
    
    return {"message": "Admin user created", "username": "admin", "password": "admin123"}

# ==================== Super Admin / Owner API ====================

@api_router.post("/super-admin/login")
async def super_admin_login(credentials: SuperAdminLogin):
    """Login for system owner/developer - with brute force protection"""
    identifier = credentials.username
    
    # Check if blocked
    if not check_brute_force(identifier):
        remaining = LOGIN_ATTEMPTS[identifier]["blocked_until"] - datetime.now(timezone.utc)
        minutes = int(remaining.total_seconds() / 60) + 1
        raise HTTPException(
            status_code=429, 
            detail=f"تم حظر تسجيل الدخول بسبب محاولات فاشلة متعددة. يرجى المحاولة بعد {minutes} دقيقة"
        )
    
    # Verify credentials
    if credentials.username == SUPER_ADMIN_USERNAME and credentials.password == SUPER_ADMIN_PASSWORD:
        reset_login_attempts(identifier)
        
        # Log successful login
        await db.login_logs.insert_one({
            "username": credentials.username,
            "role": "super_admin",
            "status": "success",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "ip": "server"
        })
        
        token = jwt.encode({
            "username": credentials.username,
            "role": "super_admin",
            "exp": datetime.now(timezone.utc).timestamp() + 86400  # 24 hours (reduced from 7 days)
        }, JWT_SECRET, algorithm=JWT_ALGORITHM)
        return {
            "token": token,
            "username": credentials.username,
            "role": "super_admin"
        }
    
    # Record failed attempt
    record_failed_login(identifier)
    attempts_left = MAX_LOGIN_ATTEMPTS - LOGIN_ATTEMPTS.get(identifier, {}).get("count", 0)
    
    # Log failed login
    await db.login_logs.insert_one({
        "username": credentials.username,
        "role": "super_admin",
        "status": "failed",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ip": "server"
    })
    
    if attempts_left > 0:
        raise HTTPException(status_code=401, detail=f"بيانات الدخول غير صحيحة. المحاولات المتبقية: {attempts_left}")
    else:
        raise HTTPException(status_code=429, detail=f"تم حظر تسجيل الدخول لمدة {BLOCK_DURATION_MINUTES} دقيقة")

@api_router.get("/super-admin/tenants")
async def get_all_tenants():
    """Get all tenants/copies of the software"""
    tenants = await db.tenants.find({}, {"_id": 0}).sort("created_at", -1).to_list(1000)
    # Add statistics for each tenant
    for tenant in tenants:
        # Check license status
        if tenant.get('license_expiry'):
            expiry_date = datetime.strptime(tenant['license_expiry'], '%Y-%m-%d')
            tenant['is_expired'] = expiry_date < datetime.now()
            tenant['days_remaining'] = (expiry_date - datetime.now()).days
        else:
            tenant['is_expired'] = True
            tenant['days_remaining'] = 0
    return tenants

@api_router.post("/super-admin/tenants")
async def create_tenant(tenant: TenantCreate):
    """Create a new tenant/copy with its own database"""
    import uuid
    tenant_id = str(uuid.uuid4())[:8].upper()
    
    # Check if email already exists
    existing = await db.tenants.find_one({"email": tenant.email})
    if existing:
        raise HTTPException(status_code=400, detail="Tenant with this email already exists")
    
    doc = tenant.model_dump()
    doc['tenant_id'] = tenant_id
    doc['is_active'] = True
    doc['created_at'] = datetime.now(timezone.utc).isoformat()
    doc['updated_at'] = datetime.now(timezone.utc).isoformat()
    
    await db.tenants.insert_one(doc)
    
    # Create tenant's own database with initial data
    tenant_db = get_tenant_db(tenant_id)
    
    # Create admin user for this tenant
    hashed_password = pwd_context.hash("admin123")
    await tenant_db.users.insert_one({
        "username": "admin",
        "full_name": "مدير النظام",
        "password_hash": hashed_password,
        "role": "admin",
        "created_at": datetime.now(timezone.utc)
    })
    
    # Create company settings
    await tenant_db.company_settings.insert_one({
        "company_name_ar": tenant.company_name_ar,
        "company_name_en": tenant.company_name_en,
        "commercial_registration": "",
        "tax_number": "",
        "address_ar": "",
        "address_en": "",
        "phone": tenant.phone,
        "email": tenant.email,
        "logo_url": "",
        "logo_base64": "",
        "primary_color": "#006d5b",
        "invoice_color": "#006d5b",
        "voucher_color": "#006d5b",
        "invoice_template": "classic",
        "voucher_template": "classic",
        "license_expiry": tenant.license_expiry,
        "app_version": "1.0.0",
        "created_at": datetime.now(timezone.utc).isoformat()
    })
    
    # Create default chart of accounts from JSON file (2145 accounts)
    if DEFAULT_CHART_OF_ACCOUNTS:
        # Prepare accounts for insertion (remove extra fields if needed)
        accounts_to_insert = []
        for acc in DEFAULT_CHART_OF_ACCOUNTS:
            accounts_to_insert.append({
                "account_code": acc["account_code"],
                "account_name_ar": acc["account_name_ar"],
                "account_name_en": acc.get("account_name_en", ""),
                "account_type": acc["account_type"],
                "parent_code": acc["parent_code"],
                "level": acc["level"],
                "is_active": acc["is_active"],
                "nature": acc.get("nature", "debit"),
                "closing_type": acc.get("closing_type", "balance_sheet"),
                "created_at": datetime.now(timezone.utc).isoformat()
            })
        await tenant_db.chart_of_accounts.insert_many(accounts_to_insert)
    
    return {
        "message": "Tenant created successfully",
        "tenant_id": tenant_id,
        "login_url": f"/tenant/{tenant_id}",
        "admin_username": "admin",
        "admin_password": "admin123"
    }

@api_router.get("/super-admin/tenants/{tenant_id}")
async def get_tenant(tenant_id: str):
    """Get tenant details"""
    tenant = await db.tenants.find_one({"tenant_id": tenant_id}, {"_id": 0})
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return tenant

@api_router.put("/super-admin/tenants/{tenant_id}")
async def update_tenant(tenant_id: str, tenant: TenantCreate):
    """Update tenant information"""
    doc = tenant.model_dump()
    doc['updated_at'] = datetime.now(timezone.utc).isoformat()
    
    result = await db.tenants.update_one(
        {"tenant_id": tenant_id},
        {"$set": doc}
    )
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    return {"message": "Tenant updated successfully"}

@api_router.put("/super-admin/tenants/{tenant_id}/toggle-status")
async def toggle_tenant_status(tenant_id: str):
    """Activate or deactivate a tenant"""
    tenant = await db.tenants.find_one({"tenant_id": tenant_id})
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    new_status = not tenant.get('is_active', True)
    await db.tenants.update_one(
        {"tenant_id": tenant_id},
        {"$set": {"is_active": new_status, "updated_at": datetime.now(timezone.utc).isoformat()}}
    )
    
    return {"message": f"Tenant {'activated' if new_status else 'deactivated'}", "is_active": new_status}

@api_router.put("/super-admin/tenants/{tenant_id}/extend-license")
async def extend_tenant_license(tenant_id: str, days: int = 30):
    """Extend tenant license by specified days"""
    tenant = await db.tenants.find_one({"tenant_id": tenant_id})
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    from datetime import timedelta
    current_expiry = datetime.strptime(tenant['license_expiry'], '%Y-%m-%d')
    if current_expiry < datetime.now():
        current_expiry = datetime.now()
    
    new_expiry = current_expiry + timedelta(days=days)
    
    await db.tenants.update_one(
        {"tenant_id": tenant_id},
        {"$set": {
            "license_expiry": new_expiry.strftime('%Y-%m-%d'),
            "updated_at": datetime.now(timezone.utc).isoformat()
        }}
    )
    
    return {
        "message": f"License extended by {days} days",
        "new_expiry": new_expiry.strftime('%Y-%m-%d')
    }

@api_router.delete("/super-admin/tenants/{tenant_id}")
async def delete_tenant(tenant_id: str):
    """Delete a tenant permanently"""
    result = await db.tenants.delete_one({"tenant_id": tenant_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return {"message": "Tenant deleted successfully"}

@api_router.get("/super-admin/statistics")
async def get_super_admin_statistics():
    """Get overall statistics for all tenants"""
    total_tenants = await db.tenants.count_documents({})
    active_tenants = await db.tenants.count_documents({"is_active": True})
    
    # Get expired tenants
    today = datetime.now().strftime('%Y-%m-%d')
    expired_tenants = await db.tenants.count_documents({"license_expiry": {"$lt": today}})
    
    # Get tenants expiring soon (within 30 days)
    from datetime import timedelta
    expiring_soon_date = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
    expiring_soon = await db.tenants.count_documents({
        "license_expiry": {"$gte": today, "$lte": expiring_soon_date}
    })
    
    # License type distribution
    standard = await db.tenants.count_documents({"license_type": "standard"})
    premium = await db.tenants.count_documents({"license_type": "premium"})
    enterprise = await db.tenants.count_documents({"license_type": "enterprise"})
    
    return {
        "total_tenants": total_tenants,
        "active_tenants": active_tenants,
        "inactive_tenants": total_tenants - active_tenants,
        "expired_tenants": expired_tenants,
        "expiring_soon": expiring_soon,
        "license_distribution": {
            "standard": standard,
            "premium": premium,
            "enterprise": enterprise
        }
    }

@api_router.get("/super-admin/login-logs")
async def get_login_logs(limit: int = 50):
    """Get recent login attempts for security monitoring"""
    logs = await db.login_logs.find({}, {"_id": 0}).sort("timestamp", -1).limit(limit).to_list(limit)
    return {
        "total": await db.login_logs.count_documents({}),
        "failed_last_24h": await db.login_logs.count_documents({
            "status": "failed",
            "timestamp": {"$gte": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()}
        }),
        "logs": logs
    }

@api_router.delete("/super-admin/login-logs")
async def clear_login_logs():
    """Clear old login logs (keep last 7 days)"""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    result = await db.login_logs.delete_many({"timestamp": {"$lt": cutoff}})
    return {"deleted": result.deleted_count}

# ==================== Tenant-Specific APIs ====================
# These endpoints work with a specific tenant's database based on X-Tenant-ID header

from fastapi import Header

async def get_current_tenant_db(x_tenant_id: str = Header(None)):
    """Get the database for the current tenant from header"""
    if x_tenant_id:
        # Verify tenant exists and is active
        tenant = await db.tenants.find_one({"tenant_id": x_tenant_id})
        if not tenant:
            raise HTTPException(status_code=404, detail="Tenant not found")
        if not tenant.get('is_active', True):
            raise HTTPException(status_code=403, detail="Tenant is deactivated")
        # Check license expiry
        if tenant.get('license_expiry'):
            expiry = datetime.strptime(tenant['license_expiry'], '%Y-%m-%d')
            if expiry < datetime.now():
                raise HTTPException(status_code=403, detail="License expired")
        return get_tenant_db(x_tenant_id)
    return db

@api_router.post("/tenant/login")
async def tenant_login(user: UserLogin, x_tenant_id: str = Header(None)):
    """Login for a specific tenant - with brute force protection"""
    identifier = f"{x_tenant_id}:{user.username}"
    
    # Check if blocked
    if not check_brute_force(identifier):
        remaining = LOGIN_ATTEMPTS[identifier]["blocked_until"] - datetime.now(timezone.utc)
        minutes = int(remaining.total_seconds() / 60) + 1
        raise HTTPException(
            status_code=429, 
            detail=f"تم حظر تسجيل الدخول بسبب محاولات فاشلة متعددة. يرجى المحاولة بعد {minutes} دقيقة"
        )
    
    tenant_db = await get_current_tenant_db(x_tenant_id)
    
    db_user = await tenant_db.users.find_one({"username": user.username})
    if not db_user or not pwd_context.verify(user.password, db_user['password_hash']):
        # Record failed attempt
        record_failed_login(identifier)
        attempts_left = MAX_LOGIN_ATTEMPTS - LOGIN_ATTEMPTS.get(identifier, {}).get("count", 0)
        
        # Log failed login
        await db.login_logs.insert_one({
            "username": user.username,
            "tenant_id": x_tenant_id,
            "role": "tenant_user",
            "status": "failed",
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        
        if attempts_left > 0:
            raise HTTPException(status_code=401, detail=f"بيانات الدخول غير صحيحة. المحاولات المتبقية: {attempts_left}")
        else:
            raise HTTPException(status_code=429, detail=f"تم حظر تسجيل الدخول لمدة {BLOCK_DURATION_MINUTES} دقيقة")
    
    # Reset attempts on successful login
    reset_login_attempts(identifier)
    
    # Log successful login
    await db.login_logs.insert_one({
        "username": user.username,
        "tenant_id": x_tenant_id,
        "role": "tenant_user",
        "status": "success",
        "timestamp": datetime.now(timezone.utc).isoformat()
    })
    
    token = jwt.encode({
        "username": user.username,
        "tenant_id": x_tenant_id,
        "exp": datetime.now(timezone.utc).timestamp() + 86400  # 24 hours
    }, JWT_SECRET, algorithm=JWT_ALGORITHM)
    
    return {
        "token": token,
        "username": user.username,
        "full_name": db_user.get('full_name', user.username),
        "tenant_id": x_tenant_id
    }

@api_router.get("/tenant/info")
async def get_tenant_info(x_tenant_id: str = Header(None)):
    """Get current tenant information"""
    if not x_tenant_id:
        raise HTTPException(status_code=400, detail="Tenant ID required")
    
    tenant = await db.tenants.find_one({"tenant_id": x_tenant_id}, {"_id": 0})
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    return tenant

# ==================== Chart of Accounts ====================

@api_router.get("/accounts", response_model=List[ChartOfAccount])
async def get_accounts(x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    accounts = await target_db.chart_of_accounts.find({}, {"_id": 0}).sort("account_code", 1).to_list(5000)
    for acc in accounts:
        if isinstance(acc.get('created_at'), str):
            acc['created_at'] = datetime.fromisoformat(acc['created_at'])
    return accounts

@api_router.post("/accounts", response_model=ChartOfAccount)
async def create_account(account: ChartOfAccount, x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    existing = await target_db.chart_of_accounts.find_one({"account_code": account.account_code})
    if existing:
        raise HTTPException(status_code=400, detail="Account code already exists")
    
    doc = account.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    await target_db.chart_of_accounts.insert_one(doc)
    return account

@api_router.put("/accounts/{account_code}", response_model=ChartOfAccount)
async def update_account(account_code: str, account: ChartOfAccount, x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    doc = account.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    result = await target_db.chart_of_accounts.replace_one({"account_code": account_code}, doc)
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Account not found")
    return account

@api_router.delete("/accounts/{account_code}")
async def delete_account(account_code: str, x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    result = await target_db.chart_of_accounts.delete_one({"account_code": account_code})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Account not found")
    return {"message": "Account deleted"}

@api_router.post("/accounts/reset-to-default")
async def reset_chart_of_accounts(x_tenant_id: str = Header(None)):
    """Reset chart of accounts to default (2145 accounts from Excel)"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    
    if not DEFAULT_CHART_OF_ACCOUNTS:
        raise HTTPException(status_code=500, detail="Default chart of accounts not loaded")
    
    # Delete all existing accounts
    await target_db.chart_of_accounts.delete_many({})
    
    # Insert default accounts
    accounts_to_insert = []
    for acc in DEFAULT_CHART_OF_ACCOUNTS:
        accounts_to_insert.append({
            "account_code": acc["account_code"],
            "account_name_ar": acc["account_name_ar"],
            "account_name_en": acc.get("account_name_en", ""),
            "account_type": acc["account_type"],
            "parent_code": acc["parent_code"],
            "level": acc["level"],
            "is_active": acc["is_active"],
            "nature": acc.get("nature", "debit"),
            "closing_type": acc.get("closing_type", "balance_sheet"),
            "created_at": datetime.now(timezone.utc).isoformat()
        })
    
    await target_db.chart_of_accounts.insert_many(accounts_to_insert)
    
    return {
        "message": "تم إعادة تعيين دليل الحسابات بنجاح",
        "message_en": "Chart of accounts reset successfully",
        "count": len(accounts_to_insert)
    }

@api_router.get("/accounts/stats")
async def get_accounts_stats(x_tenant_id: str = Header(None)):
    """Get statistics about the chart of accounts"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    
    total = await target_db.chart_of_accounts.count_documents({})
    active = await target_db.chart_of_accounts.count_documents({"is_active": True})
    
    # Count by type
    pipeline = [
        {"$group": {"_id": "$account_type", "count": {"$sum": 1}}}
    ]
    type_counts = {}
    async for doc in target_db.chart_of_accounts.aggregate(pipeline):
        type_counts[doc["_id"]] = doc["count"]
    
    return {
        "total": total,
        "active": active,
        "inactive": total - active,
        "by_type": type_counts,
        "default_available": len(DEFAULT_CHART_OF_ACCOUNTS)
    }

@api_router.post("/accounts/init")
async def init_saudi_chart_of_accounts():
    """Initialize Complete Saudi Unified Chart of Accounts"""
    existing_count = await db.chart_of_accounts.count_documents({})
    if existing_count > 0:
        return {"message": "Chart of accounts already initialized", "count": existing_count}
    
    # دليل حسابات سعودي موحد كامل
    saudi_accounts = [
        # 1. الأصول Assets (1xxxx)
        ChartOfAccount(account_code="10000", account_name_ar="الأصول", account_name_en="Assets", account_type="asset", level=1),
        
        # 1.1 الأصول المتداولة Current Assets
        ChartOfAccount(account_code="11000", account_name_ar="الأصول المتداولة", account_name_en="Current Assets", account_type="asset", parent_code="10000", level=2),
        ChartOfAccount(account_code="11100", account_name_ar="النقدية وما في حكمها", account_name_en="Cash and Cash Equivalents", account_type="asset", parent_code="11000", level=3),
        ChartOfAccount(account_code="11110", account_name_ar="الصندوق", account_name_en="Cash on Hand", account_type="asset", parent_code="11100", level=4),
        ChartOfAccount(account_code="11120", account_name_ar="البنك - الحساب الجاري", account_name_en="Bank - Current Account", account_type="asset", parent_code="11100", level=4),
        ChartOfAccount(account_code="11130", account_name_ar="البنك - حساب التوفير", account_name_en="Bank - Savings Account", account_type="asset", parent_code="11100", level=4),
        ChartOfAccount(account_code="11140", account_name_ar="النقدية في الطريق", account_name_en="Cash in Transit", account_type="asset", parent_code="11100", level=4),
        
        ChartOfAccount(account_code="11200", account_name_ar="المدينون والذمم المدينة", account_name_en="Accounts Receivable", account_type="asset", parent_code="11000", level=3),
        ChartOfAccount(account_code="11210", account_name_ar="العملاء", account_name_en="Customers", account_type="asset", parent_code="11200", level=4),
        ChartOfAccount(account_code="11220", account_name_ar="أوراق القبض", account_name_en="Notes Receivable", account_type="asset", parent_code="11200", level=4),
        ChartOfAccount(account_code="11230", account_name_ar="مخصص الديون المشكوك في تحصيلها", account_name_en="Allowance for Doubtful Accounts", account_type="asset", parent_code="11200", level=4),
        ChartOfAccount(account_code="11240", account_name_ar="دفعات مقدمة للموردين", account_name_en="Advances to Suppliers", account_type="asset", parent_code="11200", level=4),
        
        ChartOfAccount(account_code="11300", account_name_ar="المخزون", account_name_en="Inventory", account_type="asset", parent_code="11000", level=3),
        ChartOfAccount(account_code="11310", account_name_ar="مخزون البضائع", account_name_en="Merchandise Inventory", account_type="asset", parent_code="11300", level=4),
        ChartOfAccount(account_code="11320", account_name_ar="مخزون المواد الخام", account_name_en="Raw Materials Inventory", account_type="asset", parent_code="11300", level=4),
        ChartOfAccount(account_code="11330", account_name_ar="مخزون الإنتاج تحت التشغيل", account_name_en="Work in Progress", account_type="asset", parent_code="11300", level=4),
        ChartOfAccount(account_code="11340", account_name_ar="مخزون المنتجات التامة", account_name_en="Finished Goods", account_type="asset", parent_code="11300", level=4),
        
        ChartOfAccount(account_code="11400", account_name_ar="مصروفات مدفوعة مقدماً", account_name_en="Prepaid Expenses", account_type="asset", parent_code="11000", level=3),
        ChartOfAccount(account_code="11410", account_name_ar="إيجار مدفوع مقدماً", account_name_en="Prepaid Rent", account_type="asset", parent_code="11400", level=4),
        ChartOfAccount(account_code="11420", account_name_ar="تأمين مدفوع مقدماً", account_name_en="Prepaid Insurance", account_type="asset", parent_code="11400", level=4),
        
        # 1.2 الأصول غير المتداولة Non-Current Assets
        ChartOfAccount(account_code="12000", account_name_ar="الأصول غير المتداولة", account_name_en="Non-Current Assets", account_type="asset", parent_code="10000", level=2),
        ChartOfAccount(account_code="12100", account_name_ar="الأصول الثابتة", account_name_en="Fixed Assets", account_type="asset", parent_code="12000", level=3),
        ChartOfAccount(account_code="12110", account_name_ar="الأراضي", account_name_en="Land", account_type="asset", parent_code="12100", level=4),
        ChartOfAccount(account_code="12120", account_name_ar="المباني", account_name_en="Buildings", account_type="asset", parent_code="12100", level=4),
        ChartOfAccount(account_code="12121", account_name_ar="مجمع استهلاك المباني", account_name_en="Accumulated Depreciation - Buildings", account_type="asset", parent_code="12100", level=4),
        ChartOfAccount(account_code="12130", account_name_ar="السيارات", account_name_en="Vehicles", account_type="asset", parent_code="12100", level=4),
        ChartOfAccount(account_code="12131", account_name_ar="مجمع استهلاك السيارات", account_name_en="Accumulated Depreciation - Vehicles", account_type="asset", parent_code="12100", level=4),
        ChartOfAccount(account_code="12140", account_name_ar="الأثاث والمعدات المكتبية", account_name_en="Furniture and Office Equipment", account_type="asset", parent_code="12100", level=4),
        ChartOfAccount(account_code="12141", account_name_ar="مجمع استهلاك الأثاث", account_name_en="Accumulated Depreciation - Furniture", account_type="asset", parent_code="12100", level=4),
        ChartOfAccount(account_code="12150", account_name_ar="أجهزة الكمبيوتر", account_name_en="Computers", account_type="asset", parent_code="12100", level=4),
        ChartOfAccount(account_code="12151", account_name_ar="مجمع استهلاك أجهزة الكمبيوتر", account_name_en="Accumulated Depreciation - Computers", account_type="asset", parent_code="12100", level=4),
        
        ChartOfAccount(account_code="12200", account_name_ar="الأصول غير الملموسة", account_name_en="Intangible Assets", account_type="asset", parent_code="12000", level=3),
        ChartOfAccount(account_code="12210", account_name_ar="الشهرة", account_name_en="Goodwill", account_type="asset", parent_code="12200", level=4),
        ChartOfAccount(account_code="12220", account_name_ar="براءات الاختراع", account_name_en="Patents", account_type="asset", parent_code="12200", level=4),
        ChartOfAccount(account_code="12230", account_name_ar="العلامات التجارية", account_name_en="Trademarks", account_type="asset", parent_code="12200", level=4),
        
        # 2. الخصوم Liabilities (2xxxx)
        ChartOfAccount(account_code="20000", account_name_ar="الخصوم", account_name_en="Liabilities", account_type="liability", level=1),
        
        # 2.1 الخصوم المتداولة Current Liabilities
        ChartOfAccount(account_code="21000", account_name_ar="الخصوم المتداولة", account_name_en="Current Liabilities", account_type="liability", parent_code="20000", level=2),
        ChartOfAccount(account_code="21100", account_name_ar="الدائنون والذمم الدائنة", account_name_en="Accounts Payable", account_type="liability", parent_code="21000", level=3),
        ChartOfAccount(account_code="21110", account_name_ar="الموردون", account_name_en="Suppliers", account_type="liability", parent_code="21100", level=4),
        ChartOfAccount(account_code="21120", account_name_ar="أوراق الدفع", account_name_en="Notes Payable", account_type="liability", parent_code="21100", level=4),
        ChartOfAccount(account_code="21130", account_name_ar="مصروفات مستحقة", account_name_en="Accrued Expenses", account_type="liability", parent_code="21100", level=4),
        
        ChartOfAccount(account_code="21200", account_name_ar="الضرائب المستحقة", account_name_en="Taxes Payable", account_type="liability", parent_code="21000", level=3),
        ChartOfAccount(account_code="21210", account_name_ar="ضريبة القيمة المضافة المستحقة", account_name_en="VAT Payable", account_type="liability", parent_code="21200", level=4),
        ChartOfAccount(account_code="21220", account_name_ar="ضريبة الدخل المستحقة", account_name_en="Income Tax Payable", account_type="liability", parent_code="21200", level=4),
        ChartOfAccount(account_code="21230", account_name_ar="الزكاة المستحقة", account_name_en="Zakat Payable", account_type="liability", parent_code="21200", level=4),
        
        ChartOfAccount(account_code="21300", account_name_ar="رواتب وأجور مستحقة", account_name_en="Salaries and Wages Payable", account_type="liability", parent_code="21000", level=3),
        ChartOfAccount(account_code="21310", account_name_ar="رواتب مستحقة", account_name_en="Salaries Payable", account_type="liability", parent_code="21300", level=4),
        ChartOfAccount(account_code="21320", account_name_ar="مستحقات التأمينات الاجتماعية", account_name_en="Social Insurance Payable", account_type="liability", parent_code="21300", level=4),
        
        # 2.2 الخصوم غير المتداولة Non-Current Liabilities
        ChartOfAccount(account_code="22000", account_name_ar="الخصوم غير المتداولة", account_name_en="Non-Current Liabilities", account_type="liability", parent_code="20000", level=2),
        ChartOfAccount(account_code="22100", account_name_ar="قروض طويلة الأجل", account_name_en="Long-term Loans", account_type="liability", parent_code="22000", level=3),
        ChartOfAccount(account_code="22110", account_name_ar="قروض بنكية طويلة الأجل", account_name_en="Long-term Bank Loans", account_type="liability", parent_code="22100", level=4),
        
        # 3. حقوق الملكية Equity (3xxxx)
        ChartOfAccount(account_code="30000", account_name_ar="حقوق الملكية", account_name_en="Equity", account_type="equity", level=1),
        ChartOfAccount(account_code="31000", account_name_ar="رأس المال", account_name_en="Capital", account_type="equity", parent_code="30000", level=2),
        ChartOfAccount(account_code="31100", account_name_ar="رأس المال المدفوع", account_name_en="Paid-in Capital", account_type="equity", parent_code="31000", level=3),
        ChartOfAccount(account_code="32000", account_name_ar="الأرباح المحتجزة", account_name_en="Retained Earnings", account_type="equity", parent_code="30000", level=2),
        ChartOfAccount(account_code="32100", account_name_ar="أرباح العام الحالي", account_name_en="Current Year Earnings", account_type="equity", parent_code="32000", level=3),
        ChartOfAccount(account_code="32200", account_name_ar="أرباح السنوات السابقة", account_name_en="Prior Years Retained Earnings", account_type="equity", parent_code="32000", level=3),
        ChartOfAccount(account_code="33000", account_name_ar="مسحوبات الشركاء", account_name_en="Partners' Drawings", account_type="equity", parent_code="30000", level=2),
        
        # 4. الإيرادات Revenue (4xxxx)
        ChartOfAccount(account_code="40000", account_name_ar="الإيرادات", account_name_en="Revenue", account_type="revenue", level=1),
        ChartOfAccount(account_code="41000", account_name_ar="إيرادات المبيعات", account_name_en="Sales Revenue", account_type="revenue", parent_code="40000", level=2),
        ChartOfAccount(account_code="41100", account_name_ar="مبيعات البضائع", account_name_en="Merchandise Sales", account_type="revenue", parent_code="41000", level=3),
        ChartOfAccount(account_code="41200", account_name_ar="مبيعات الخدمات", account_name_en="Service Revenue", account_type="revenue", parent_code="41000", level=3),
        ChartOfAccount(account_code="41300", account_name_ar="مردودات المبيعات", account_name_en="Sales Returns", account_type="revenue", parent_code="41000", level=3),
        ChartOfAccount(account_code="41400", account_name_ar="خصم مسموح به", account_name_en="Sales Discounts", account_type="revenue", parent_code="41000", level=3),
        
        ChartOfAccount(account_code="42000", account_name_ar="إيرادات أخرى", account_name_en="Other Revenue", account_type="revenue", parent_code="40000", level=2),
        ChartOfAccount(account_code="42100", account_name_ar="إيرادات الفوائد", account_name_en="Interest Income", account_type="revenue", parent_code="42000", level=3),
        ChartOfAccount(account_code="42200", account_name_ar="إيرادات الإيجار", account_name_en="Rental Income", account_type="revenue", parent_code="42000", level=3),
        ChartOfAccount(account_code="42300", account_name_ar="أرباح بيع أصول", account_name_en="Gain on Sale of Assets", account_type="revenue", parent_code="42000", level=3),
        
        # 5. المصروفات Expenses (5xxxx)
        ChartOfAccount(account_code="50000", account_name_ar="المصروفات", account_name_en="Expenses", account_type="expense", level=1),
        
        ChartOfAccount(account_code="51000", account_name_ar="تكلفة المبيعات", account_name_en="Cost of Sales", account_type="expense", parent_code="50000", level=2),
        ChartOfAccount(account_code="51100", account_name_ar="تكلفة البضاعة المباعة", account_name_en="Cost of Goods Sold", account_type="expense", parent_code="51000", level=3),
        ChartOfAccount(account_code="51200", account_name_ar="مشتريات", account_name_en="Purchases", account_type="expense", parent_code="51000", level=3),
        ChartOfAccount(account_code="51300", account_name_ar="مردودات المشتريات", account_name_en="Purchase Returns", account_type="expense", parent_code="51000", level=3),
        ChartOfAccount(account_code="51400", account_name_ar="خصم مكتسب", account_name_en="Purchase Discounts", account_type="expense", parent_code="51000", level=3),
        
        ChartOfAccount(account_code="52000", account_name_ar="المصروفات الإدارية", account_name_en="Administrative Expenses", account_type="expense", parent_code="50000", level=2),
        ChartOfAccount(account_code="52100", account_name_ar="الرواتب والأجور", account_name_en="Salaries and Wages", account_type="expense", parent_code="52000", level=3),
        ChartOfAccount(account_code="52200", account_name_ar="إيجار المكتب", account_name_en="Office Rent", account_type="expense", parent_code="52000", level=3),
        ChartOfAccount(account_code="52300", account_name_ar="الكهرباء والماء", account_name_en="Utilities", account_type="expense", parent_code="52000", level=3),
        ChartOfAccount(account_code="52400", account_name_ar="القرطاسية والمطبوعات", account_name_en="Stationery and Printing", account_type="expense", parent_code="52000", level=3),
        ChartOfAccount(account_code="52500", account_name_ar="صيانة وإصلاحات", account_name_en="Maintenance and Repairs", account_type="expense", parent_code="52000", level=3),
        ChartOfAccount(account_code="52600", account_name_ar="التأمين", account_name_en="Insurance", account_type="expense", parent_code="52000", level=3),
        ChartOfAccount(account_code="52700", account_name_ar="الاتصالات", account_name_en="Communication Expenses", account_type="expense", parent_code="52000", level=3),
        ChartOfAccount(account_code="52800", account_name_ar="مصروفات قانونية ومهنية", account_name_en="Legal and Professional Fees", account_type="expense", parent_code="52000", level=3),
        
        ChartOfAccount(account_code="53000", account_name_ar="المصروفات التشغيلية", account_name_en="Operating Expenses", account_type="expense", parent_code="50000", level=2),
        ChartOfAccount(account_code="53100", account_name_ar="مصروفات البيع والتسويق", account_name_en="Sales and Marketing Expenses", account_type="expense", parent_code="53000", level=3),
        ChartOfAccount(account_code="53200", account_name_ar="مصروفات النقل والشحن", account_name_en="Transportation and Shipping", account_type="expense", parent_code="53000", level=3),
        ChartOfAccount(account_code="53300", account_name_ar="عمولات المبيعات", account_name_en="Sales Commissions", account_type="expense", parent_code="53000", level=3),
        ChartOfAccount(account_code="53400", account_name_ar="الإعلان والترويج", account_name_en="Advertising and Promotion", account_type="expense", parent_code="53000", level=3),
        
        ChartOfAccount(account_code="54000", account_name_ar="مصروفات أخرى", account_name_en="Other Expenses", account_type="expense", parent_code="50000", level=2),
        ChartOfAccount(account_code="54100", account_name_ar="استهلاك الأصول الثابتة", account_name_en="Depreciation Expense", account_type="expense", parent_code="54000", level=3),
        ChartOfAccount(account_code="54200", account_name_ar="مصروفات الفوائد", account_name_en="Interest Expense", account_type="expense", parent_code="54000", level=3),
        ChartOfAccount(account_code="54300", account_name_ar="خسائر بيع أصول", account_name_en="Loss on Sale of Assets", account_type="expense", parent_code="54000", level=3),
        ChartOfAccount(account_code="54400", account_name_ar="ديون معدومة", account_name_en="Bad Debts Expense", account_type="expense", parent_code="54000", level=3),
        ChartOfAccount(account_code="54500", account_name_ar="مصروفات بنكية", account_name_en="Bank Charges", account_type="expense", parent_code="54000", level=3),
        ChartOfAccount(account_code="54600", account_name_ar="غرامات وجزاءات", account_name_en="Penalties and Fines", account_type="expense", parent_code="54000", level=3),
    ]
    
    for account in saudi_accounts:
        doc = account.model_dump()
        doc['created_at'] = doc['created_at'].isoformat()
        await db.chart_of_accounts.insert_one(doc)
    
    return {"message": f"Initialized {len(saudi_accounts)} accounts - Complete Saudi Unified Chart of Accounts"}

# ==================== Opening Balances ====================

@api_router.get("/opening-balances", response_model=List[OpeningBalance])
async def get_opening_balances(fiscal_year: str = "2025", x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    balances = await target_db.opening_balances.find({"fiscal_year": fiscal_year}, {"_id": 0}).to_list(1000)
    for bal in balances:
        if isinstance(bal.get('created_at'), str):
            bal['created_at'] = datetime.fromisoformat(bal['created_at'])
    return balances

@api_router.post("/opening-balances", response_model=OpeningBalance)
async def create_opening_balance(balance: OpeningBalance, x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    existing = await target_db.opening_balances.find_one({
        "account_code": balance.account_code,
        "fiscal_year": balance.fiscal_year
    })
    if existing:
        raise HTTPException(status_code=400, detail="Opening balance already exists for this account and fiscal year")
    
    doc = balance.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    await target_db.opening_balances.insert_one(doc)
    return balance

@api_router.put("/opening-balances/{account_code}")
async def update_opening_balance(account_code: str, balance: OpeningBalance, x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    doc = balance.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    result = await target_db.opening_balances.replace_one(
        {"account_code": account_code, "fiscal_year": balance.fiscal_year},
        doc
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Opening balance not found")
    return balance

@api_router.delete("/opening-balances/{account_code}")
async def delete_opening_balance(account_code: str, fiscal_year: str = "2025", x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    result = await target_db.opening_balances.delete_one(
        {"account_code": account_code, "fiscal_year": fiscal_year}
    )
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Opening balance not found")
    return {"message": "Opening balance deleted"}

# ==================== Customers ====================

@api_router.get("/customers", response_model=List[Customer])
async def get_customers(x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    customers = await target_db.customers.find({}, {"_id": 0}).to_list(1000)
    for cust in customers:
        if isinstance(cust.get('created_at'), str):
            cust['created_at'] = datetime.fromisoformat(cust['created_at'])
    return customers

@api_router.post("/customers", response_model=Customer)
async def create_customer(customer: Customer, x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    existing = await target_db.customers.find_one({"customer_code": customer.customer_code})
    if existing:
        raise HTTPException(status_code=400, detail="Customer code already exists")
    
    doc = customer.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    await target_db.customers.insert_one(doc)
    return customer

@api_router.put("/customers/{customer_code}", response_model=Customer)
async def update_customer(customer_code: str, customer: Customer, x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    doc = customer.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    result = await target_db.customers.replace_one({"customer_code": customer_code}, doc)
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer

@api_router.delete("/customers/{customer_code}")
async def delete_customer(customer_code: str, x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    result = await target_db.customers.delete_one({"customer_code": customer_code})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Customer not found")
    return {"message": "Customer deleted"}

# ==================== Invoices ====================

@api_router.get("/invoices", response_model=List[Invoice])
async def get_invoices(x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    invoices = await target_db.invoices.find({}, {"_id": 0}).sort("invoice_date", -1).to_list(1000)
    for inv in invoices:
        if isinstance(inv.get('created_at'), str):
            inv['created_at'] = datetime.fromisoformat(inv['created_at'])
        if isinstance(inv.get('invoice_date'), str):
            inv['invoice_date'] = datetime.fromisoformat(inv['invoice_date'])
    return invoices

@api_router.post("/invoices", response_model=Invoice)
async def create_invoice(invoice: Invoice, x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    existing = await target_db.invoices.find_one({"invoice_number": invoice.invoice_number})
    if existing:
        raise HTTPException(status_code=400, detail="Invoice number already exists")
    
    doc = invoice.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    doc['invoice_date'] = doc['invoice_date'].isoformat()
    await target_db.invoices.insert_one(doc)
    return invoice

@api_router.get("/invoices/{invoice_number}", response_model=Invoice)
async def get_invoice(invoice_number: str, x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    invoice = await target_db.invoices.find_one({"invoice_number": invoice_number}, {"_id": 0})
    if not invoice:
        raise HTTPException(status_code=404, detail="Invoice not found")
    if isinstance(invoice.get('created_at'), str):
        invoice['created_at'] = datetime.fromisoformat(invoice['created_at'])
    if isinstance(invoice.get('invoice_date'), str):
        invoice['invoice_date'] = datetime.fromisoformat(invoice['invoice_date'])
    return invoice

# ==================== Journal Entries ====================

@api_router.get("/journal-entries/next-number")
async def get_next_journal_entry_number(x_tenant_id: str = Header(None)):
    """Get the next available journal entry number"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    
    # Get current year
    current_year = datetime.now().year
    prefix = f"JE-{current_year}-"
    
    # Find the highest entry number for this year
    latest = await target_db.journal_entries.find(
        {"entry_number": {"$regex": f"^{prefix}"}},
        {"entry_number": 1, "_id": 0}
    ).sort("entry_number", -1).limit(1).to_list(1)
    
    if latest and latest[0].get('entry_number'):
        try:
            last_num = int(latest[0]['entry_number'].split('-')[-1])
            next_num = last_num + 1
        except:
            next_num = 1
    else:
        next_num = 1
    
    return {"next_number": f"{prefix}{next_num:05d}"}

@api_router.get("/journal-entries", response_model=List[JournalEntry])
async def get_journal_entries(x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    entries = await target_db.journal_entries.find({}, {"_id": 0}).sort("entry_date", -1).to_list(1000)
    for entry in entries:
        if isinstance(entry.get('created_at'), str):
            entry['created_at'] = datetime.fromisoformat(entry['created_at'])
        if isinstance(entry.get('entry_date'), str):
            entry['entry_date'] = datetime.fromisoformat(entry['entry_date'])
    return entries

@api_router.post("/journal-entries", response_model=JournalEntry)
async def create_journal_entry(entry: JournalEntry, x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    if entry.total_debit != entry.total_credit:
        raise HTTPException(status_code=400, detail="Debits must equal credits")
    
    existing = await target_db.journal_entries.find_one({"entry_number": entry.entry_number})
    if existing:
        raise HTTPException(status_code=400, detail="Entry number already exists")
    
    doc = entry.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    doc['entry_date'] = doc['entry_date'].isoformat()
    await target_db.journal_entries.insert_one(doc)
    return entry

# ==================== Receipt Vouchers ====================

@api_router.get("/receipt-vouchers", response_model=List[ReceiptVoucher])
async def get_receipt_vouchers(x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    vouchers = await target_db.receipt_vouchers.find({}, {"_id": 0}).sort("voucher_date", -1).to_list(1000)
    for v in vouchers:
        if isinstance(v.get('created_at'), str):
            v['created_at'] = datetime.fromisoformat(v['created_at'])
        if isinstance(v.get('voucher_date'), str):
            v['voucher_date'] = datetime.fromisoformat(v['voucher_date'])
    return vouchers

@api_router.post("/receipt-vouchers", response_model=ReceiptVoucher)
async def create_receipt_voucher(voucher: ReceiptVoucher, x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    existing = await target_db.receipt_vouchers.find_one({"voucher_number": voucher.voucher_number})
    if existing:
        raise HTTPException(status_code=400, detail="Voucher number already exists")
    
    doc = voucher.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    doc['voucher_date'] = doc['voucher_date'].isoformat()
    await target_db.receipt_vouchers.insert_one(doc)
    return voucher

# ==================== Payment Vouchers ====================

@api_router.get("/payment-vouchers", response_model=List[PaymentVoucher])
async def get_payment_vouchers(x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    vouchers = await target_db.payment_vouchers.find({}, {"_id": 0}).sort("voucher_date", -1).to_list(1000)
    for v in vouchers:
        if isinstance(v.get('created_at'), str):
            v['created_at'] = datetime.fromisoformat(v['created_at'])
        if isinstance(v.get('voucher_date'), str):
            v['voucher_date'] = datetime.fromisoformat(v['voucher_date'])
    return vouchers

@api_router.post("/payment-vouchers", response_model=PaymentVoucher)
async def create_payment_voucher(voucher: PaymentVoucher, x_tenant_id: str = Header(None)):
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    existing = await target_db.payment_vouchers.find_one({"voucher_number": voucher.voucher_number})
    if existing:
        raise HTTPException(status_code=400, detail="Voucher number already exists")
    
    doc = voucher.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    doc['voucher_date'] = doc['voucher_date'].isoformat()
    await target_db.payment_vouchers.insert_one(doc)
    return voucher

# ==================== Reports ====================

@api_router.get("/reports/trial-balance")
async def get_trial_balance(fiscal_year: str = "2025", x_tenant_id: str = Header(None)):
    """Generate Trial Balance Report with Opening and Closing Balances"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    accounts = await target_db.chart_of_accounts.find({"is_active": True}, {"_id": 0}).sort("account_code", 1).to_list(1000)
    opening_balances = await target_db.opening_balances.find({"fiscal_year": fiscal_year}, {"_id": 0}).to_list(1000)
    journal_entries = await target_db.journal_entries.find({}, {"_id": 0}).to_list(10000)
    receipts = await target_db.receipt_vouchers.find({}, {"_id": 0}).to_list(1000)
    payments = await target_db.payment_vouchers.find({}, {"_id": 0}).to_list(1000)
    
    # Create opening balances map
    opening_map = {}
    for ob in opening_balances:
        opening_map[ob['account_code']] = {
            'debit': ob.get('debit', 0),
            'credit': ob.get('credit', 0)
        }
    
    # Calculate movement for each account (from transactions only)
    movement_balances = {}
    
    # Add journal entries movement
    for entry in journal_entries:
        for line in entry.get('lines', []):
            acc_code = line['account_code']
            if acc_code not in movement_balances:
                movement_balances[acc_code] = {'debit': 0, 'credit': 0}
            movement_balances[acc_code]['debit'] += line.get('debit', 0)
            movement_balances[acc_code]['credit'] += line.get('credit', 0)
    
    # Add receipts movement
    for receipt in receipts:
        acc_code = receipt['account_code']
        if acc_code not in movement_balances:
            movement_balances[acc_code] = {'debit': 0, 'credit': 0}
        movement_balances[acc_code]['debit'] += receipt['amount']
    
    # Add payments movement
    for payment in payments:
        acc_code = payment['account_code']
        if acc_code not in movement_balances:
            movement_balances[acc_code] = {'debit': 0, 'credit': 0}
        movement_balances[acc_code]['credit'] += payment['amount']
    
    # Build trial balance with opening, movement, and closing
    trial_balance = []
    total_opening_debit = 0
    total_opening_credit = 0
    total_movement_debit = 0
    total_movement_credit = 0
    total_ending_debit = 0
    total_ending_credit = 0
    
    for account in accounts:
        acc_code = account['account_code']
        
        # Get opening balance
        opening_debit = opening_map.get(acc_code, {}).get('debit', 0)
        opening_credit = opening_map.get(acc_code, {}).get('credit', 0)
        
        # Get movement
        movement_debit = movement_balances.get(acc_code, {}).get('debit', 0)
        movement_credit = movement_balances.get(acc_code, {}).get('credit', 0)
        
        # Skip if no activity
        if opening_debit == 0 and opening_credit == 0 and movement_debit == 0 and movement_credit == 0:
            continue
        
        # Calculate ending balance (net)
        total_debit = opening_debit + movement_debit
        total_credit = opening_credit + movement_credit
        net_balance = total_debit - total_credit
        
        # Determine debit or credit ending balance
        ending_debit = net_balance if net_balance > 0 else 0
        ending_credit = abs(net_balance) if net_balance < 0 else 0
        
        trial_balance.append({
            'account_code': acc_code,
            'account_name_ar': account['account_name_ar'],
            'account_name_en': account['account_name_en'],
            'account_type': account['account_type'],
            'opening_debit': opening_debit,
            'opening_credit': opening_credit,
            'debit': movement_debit,
            'credit': movement_credit,
            'ending_debit': ending_debit,
            'ending_credit': ending_credit,
            'balance': net_balance
        })
        
        total_opening_debit += opening_debit
        total_opening_credit += opening_credit
        total_movement_debit += movement_debit
        total_movement_credit += movement_credit
        total_ending_debit += ending_debit
        total_ending_credit += ending_credit
    
    return {
        'trial_balance': trial_balance,
        'total_opening_debit': total_opening_debit,
        'total_opening_credit': total_opening_credit,
        'total_debit': total_movement_debit,
        'total_credit': total_movement_credit,
        'total_ending_debit': total_ending_debit,
        'total_ending_credit': total_ending_credit,
        'fiscal_year': fiscal_year
    }

@api_router.get("/reports/account-statement/{account_code}")
async def get_account_statement(account_code: str, fiscal_year: str = "2025", x_tenant_id: str = Header(None)):
    """Generate Account Statement"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    account = await target_db.chart_of_accounts.find_one({"account_code": account_code}, {"_id": 0})
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    
    transactions = []
    balance = 0
    
    # Opening balance
    opening = await target_db.opening_balances.find_one({"account_code": account_code, "fiscal_year": fiscal_year}, {"_id": 0})
    if opening:
        balance = opening.get('debit', 0) - opening.get('credit', 0)
        transactions.append({
            'date': f"{fiscal_year}-01-01",
            'description': 'Opening Balance',
            'debit': opening.get('debit', 0),
            'credit': opening.get('credit', 0),
            'balance': balance
        })
    
    # Journal entries
    journal_entries = await target_db.journal_entries.find({}, {"_id": 0}).sort("entry_date", 1).to_list(10000)
    for entry in journal_entries:
        for line in entry.get('lines', []):
            if line['account_code'] == account_code:
                balance += line.get('debit', 0) - line.get('credit', 0)
                transactions.append({
                    'date': entry['entry_date'],
                    'description': line['description'],
                    'debit': line.get('debit', 0),
                    'credit': line.get('credit', 0),
                    'balance': balance
                })
    
    # Receipts
    receipts = await target_db.receipt_vouchers.find({"account_code": account_code}, {"_id": 0}).sort("voucher_date", 1).to_list(1000)
    for receipt in receipts:
        balance += receipt['amount']
        transactions.append({
            'date': receipt['voucher_date'],
            'description': f"Receipt: {receipt['description']}",
            'debit': receipt['amount'],
            'credit': 0,
            'balance': balance
        })
    
    # Payments
    payments = await target_db.payment_vouchers.find({"account_code": account_code}, {"_id": 0}).sort("voucher_date", 1).to_list(1000)
    for payment in payments:
        balance -= payment['amount']
        transactions.append({
            'date': payment['voucher_date'],
            'description': f"Payment: {payment['description']}",
            'debit': 0,
            'credit': payment['amount'],
            'balance': balance
        })
    
    return {
        'account': account,
        'transactions': transactions,
        'final_balance': balance
    }

@api_router.get("/reports/general-ledger")
async def get_general_ledger(fiscal_year: str = "2025", x_tenant_id: str = Header(None)):
    """Generate General Ledger Report - Optimized"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    # Fetch all accounts
    accounts = await target_db.chart_of_accounts.find({"is_active": True}, {"_id": 0}).sort("account_code", 1).to_list(1000)
    
    # Pre-fetch all data in bulk (optimization: 4 queries instead of N*4)
    opening_balances = await target_db.opening_balances.find({"fiscal_year": fiscal_year}, {"_id": 0}).to_list(10000)
    journal_entries = await target_db.journal_entries.find({}, {"_id": 0}).to_list(10000)
    receipts = await target_db.receipt_vouchers.find({}, {"_id": 0}).to_list(10000)
    payments = await target_db.payment_vouchers.find({}, {"_id": 0}).to_list(10000)
    
    # Build dictionaries for quick lookup
    opening_map = {ob['account_code']: ob for ob in opening_balances}
    
    ledger = []
    for account in accounts:
        acc_code = account['account_code']
        transactions = []
        balance = 0
        
        # Opening balance
        if acc_code in opening_map:
            ob = opening_map[acc_code]
            balance = ob.get('debit', 0) - ob.get('credit', 0)
            transactions.append({
                'date': f"{fiscal_year}-01-01",
                'description': 'Opening Balance',
                'debit': ob.get('debit', 0),
                'credit': ob.get('credit', 0),
                'balance': balance
            })
        
        # Journal entries
        for entry in journal_entries:
            for line in entry.get('lines', []):
                if line['account_code'] == acc_code:
                    balance += line.get('debit', 0) - line.get('credit', 0)
                    transactions.append({
                        'date': entry['entry_date'],
                        'description': line['description'],
                        'debit': line.get('debit', 0),
                        'credit': line.get('credit', 0),
                        'balance': balance
                    })
        
        # Receipts
        for receipt in receipts:
            if receipt['account_code'] == acc_code:
                balance += receipt['amount']
                transactions.append({
                    'date': receipt['voucher_date'],
                    'description': f"Receipt: {receipt['description']}",
                    'debit': receipt['amount'],
                    'credit': 0,
                    'balance': balance
                })
        
        # Payments
        for payment in payments:
            if payment['account_code'] == acc_code:
                balance -= payment['amount']
                transactions.append({
                    'date': payment['voucher_date'],
                    'description': f"Payment: {payment['description']}",
                    'debit': 0,
                    'credit': payment['amount'],
                    'balance': balance
                })
        
        ledger.append({
            'account': account,
            'transactions': transactions,
            'final_balance': balance
        })
    
    return {'ledger': ledger, 'fiscal_year': fiscal_year}

# ==================== Export Reports ====================

@api_router.get("/reports/export/trial-balance/pdf")
async def export_trial_balance_pdf(fiscal_year: str = "2025", x_tenant_id: str = Header(None)):
    data = await get_trial_balance(fiscal_year, x_tenant_id)
    pdf_buffer = generate_trial_balance_pdf(data)
    return StreamingResponse(pdf_buffer, media_type="application/pdf", headers={
        "Content-Disposition": f"attachment; filename=trial_balance_{fiscal_year}.pdf"
    })

@api_router.get("/reports/export/trial-balance/excel")
async def export_trial_balance_excel(fiscal_year: str = "2025", x_tenant_id: str = Header(None)):
    data = await get_trial_balance(fiscal_year, x_tenant_id)
    excel_buffer = generate_trial_balance_excel(data)
    return StreamingResponse(excel_buffer, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={
        "Content-Disposition": f"attachment; filename=trial_balance_{fiscal_year}.xlsx"
    })

@api_router.get("/reports/export/trial-balance/word")
async def export_trial_balance_word(fiscal_year: str = "2025", x_tenant_id: str = Header(None)):
    data = await get_trial_balance(fiscal_year, x_tenant_id)
    word_buffer = generate_trial_balance_word(data)
    return StreamingResponse(word_buffer, media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document", headers={
        "Content-Disposition": f"attachment; filename=trial_balance_{fiscal_year}.docx"
    })

@api_router.get("/reports/export/account-statement/pdf/{account_code}")
async def export_account_statement_pdf(account_code: str, fiscal_year: str = "2025", x_tenant_id: str = Header(None)):
    data = await get_account_statement(account_code, fiscal_year, x_tenant_id)
    pdf_buffer = generate_account_statement_pdf(data)
    return StreamingResponse(pdf_buffer, media_type="application/pdf", headers={
        "Content-Disposition": f"attachment; filename=account_statement_{account_code}_{fiscal_year}.pdf"
    })

@api_router.get("/reports/export/account-statement/excel/{account_code}")
async def export_account_statement_excel(account_code: str, fiscal_year: str = "2025", x_tenant_id: str = Header(None)):
    data = await get_account_statement(account_code, fiscal_year, x_tenant_id)
    excel_buffer = generate_account_statement_excel(data)
    return StreamingResponse(excel_buffer, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={
        "Content-Disposition": f"attachment; filename=account_statement_{account_code}_{fiscal_year}.xlsx"
    })

@api_router.get("/reports/export/account-statement/word/{account_code}")
async def export_account_statement_word(account_code: str, fiscal_year: str = "2025", x_tenant_id: str = Header(None)):
    data = await get_account_statement(account_code, fiscal_year, x_tenant_id)
    word_buffer = generate_account_statement_word(data)
    return StreamingResponse(word_buffer, media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document", headers={
        "Content-Disposition": f"attachment; filename=account_statement_{account_code}_{fiscal_year}.docx"
    })

@api_router.get("/reports/export/general-ledger/pdf")
async def export_general_ledger_pdf(fiscal_year: str = "2025", x_tenant_id: str = Header(None)):
    data = await get_general_ledger(fiscal_year, x_tenant_id)
    pdf_buffer = generate_general_ledger_pdf(data)
    return StreamingResponse(pdf_buffer, media_type="application/pdf", headers={
        "Content-Disposition": f"attachment; filename=general_ledger_{fiscal_year}.pdf"
    })

@api_router.get("/reports/export/general-ledger/excel")
async def export_general_ledger_excel(fiscal_year: str = "2025", x_tenant_id: str = Header(None)):
    data = await get_general_ledger(fiscal_year, x_tenant_id)
    excel_buffer = generate_general_ledger_excel(data)
    return StreamingResponse(excel_buffer, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={
        "Content-Disposition": f"attachment; filename=general_ledger_{fiscal_year}.xlsx"
    })

@api_router.get("/reports/export/general-ledger/word")
async def export_general_ledger_word(fiscal_year: str = "2025", x_tenant_id: str = Header(None)):
    data = await get_general_ledger(fiscal_year, x_tenant_id)
    word_buffer = generate_general_ledger_word(data)
    return StreamingResponse(word_buffer, media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document", headers={
        "Content-Disposition": f"attachment; filename=general_ledger_{fiscal_year}.docx"
    })

# ==================== Dashboard Stats ====================

@api_router.get("/dashboard/stats")
async def get_dashboard_stats():
    total_accounts = await db.chart_of_accounts.count_documents({})
    total_customers = await db.customers.count_documents({})
    total_invoices = await db.invoices.count_documents({})
    total_journal_entries = await db.journal_entries.count_documents({})
    
    # Calculate total revenue
    invoices = await db.invoices.find({}, {"_id": 0}).to_list(10000)
    total_revenue = sum(inv.get('total_amount', 0) for inv in invoices)
    
    return {
        'total_accounts': total_accounts,
        'total_customers': total_customers,
        'total_invoices': total_invoices,
        'total_journal_entries': total_journal_entries,
        'total_revenue': total_revenue
    }

# ==================== Financial Statements ====================

@api_router.get("/reports/income-statement")
async def get_income_statement(fiscal_year: str = "2025", start_date: str = None, end_date: str = None):
    """Generate Income Statement (قائمة الدخل)"""
    
    # Get all revenue and expense accounts
    revenue_accounts = await db.chart_of_accounts.find(
        {"account_type": "revenue", "is_active": True}, {"_id": 0}
    ).sort("account_code", 1).to_list(100)
    
    expense_accounts = await db.chart_of_accounts.find(
        {"account_type": "expense", "is_active": True}, {"_id": 0}
    ).sort("account_code", 1).to_list(100)
    
    # Get all journal entries
    journal_entries = await db.journal_entries.find({}, {"_id": 0}).to_list(10000)
    
    # Get invoices for revenue
    invoices = await db.invoices.find({}, {"_id": 0}).to_list(10000)
    
    # Calculate totals for each account
    def calculate_account_balance(account_code, entries, is_revenue=True):
        total = 0
        for entry in entries:
            for line in entry.get('lines', []):
                if line['account_code'] == account_code:
                    if is_revenue:
                        total += line.get('credit', 0) - line.get('debit', 0)
                    else:
                        total += line.get('debit', 0) - line.get('credit', 0)
        return total
    
    # Revenue section
    revenue_items = []
    total_revenue = 0
    for acc in revenue_accounts:
        if acc['level'] > 1:  # Skip parent accounts
            balance = calculate_account_balance(acc['account_code'], journal_entries, True)
            if balance != 0:
                revenue_items.append({
                    'account_code': acc['account_code'],
                    'account_name_ar': acc['account_name_ar'],
                    'account_name_en': acc['account_name_en'],
                    'amount': balance
                })
                total_revenue += balance
    
    # Add invoice revenue if not already in journal entries
    invoice_revenue = sum(inv.get('subtotal', 0) for inv in invoices)
    if invoice_revenue > 0 and total_revenue == 0:
        total_revenue = invoice_revenue
        revenue_items.append({
            'account_code': '41100',
            'account_name_ar': 'مبيعات البضائع',
            'account_name_en': 'Sales Revenue',
            'amount': invoice_revenue
        })
    
    # Expense section
    expense_items = []
    total_expenses = 0
    
    # Group expenses by category
    cost_of_sales = []
    operating_expenses = []
    admin_expenses = []
    other_expenses = []
    
    for acc in expense_accounts:
        if acc['level'] > 1:  # Skip parent accounts
            balance = calculate_account_balance(acc['account_code'], journal_entries, False)
            if balance != 0:
                item = {
                    'account_code': acc['account_code'],
                    'account_name_ar': acc['account_name_ar'],
                    'account_name_en': acc['account_name_en'],
                    'amount': balance
                }
                total_expenses += balance
                
                # Categorize
                if acc['account_code'].startswith('51'):
                    cost_of_sales.append(item)
                elif acc['account_code'].startswith('52'):
                    operating_expenses.append(item)
                elif acc['account_code'].startswith('53'):
                    admin_expenses.append(item)
                else:
                    other_expenses.append(item)
    
    gross_profit = total_revenue - sum(item['amount'] for item in cost_of_sales)
    operating_income = gross_profit - sum(item['amount'] for item in operating_expenses) - sum(item['amount'] for item in admin_expenses)
    net_income = total_revenue - total_expenses
    
    return {
        'fiscal_year': fiscal_year,
        'report_date': datetime.now(timezone.utc).isoformat(),
        'revenue': {
            'items': revenue_items,
            'total': total_revenue
        },
        'cost_of_sales': {
            'items': cost_of_sales,
            'total': sum(item['amount'] for item in cost_of_sales)
        },
        'gross_profit': gross_profit,
        'operating_expenses': {
            'items': operating_expenses,
            'total': sum(item['amount'] for item in operating_expenses)
        },
        'admin_expenses': {
            'items': admin_expenses,
            'total': sum(item['amount'] for item in admin_expenses)
        },
        'other_expenses': {
            'items': other_expenses,
            'total': sum(item['amount'] for item in other_expenses)
        },
        'operating_income': operating_income,
        'total_expenses': total_expenses,
        'net_income': net_income
    }


@api_router.get("/reports/balance-sheet")
async def get_balance_sheet(fiscal_year: str = "2025"):
    """Generate Balance Sheet / Statement of Financial Position (قائمة المركز المالي)"""
    
    # Get all accounts by type
    asset_accounts = await db.chart_of_accounts.find(
        {"account_type": "asset", "is_active": True}, {"_id": 0}
    ).sort("account_code", 1).to_list(100)
    
    liability_accounts = await db.chart_of_accounts.find(
        {"account_type": "liability", "is_active": True}, {"_id": 0}
    ).sort("account_code", 1).to_list(100)
    
    equity_accounts = await db.chart_of_accounts.find(
        {"account_type": "equity", "is_active": True}, {"_id": 0}
    ).sort("account_code", 1).to_list(100)
    
    # Get opening balances and journal entries
    opening_balances = await db.opening_balances.find({"fiscal_year": fiscal_year}, {"_id": 0}).to_list(10000)
    journal_entries = await db.journal_entries.find({}, {"_id": 0}).to_list(10000)
    receipts = await db.receipt_vouchers.find({}, {"_id": 0}).to_list(10000)
    payments = await db.payment_vouchers.find({}, {"_id": 0}).to_list(10000)
    
    opening_map = {ob['account_code']: ob for ob in opening_balances}
    
    def calculate_balance(account_code, is_debit_normal=True):
        balance = 0
        
        # Opening balance
        if account_code in opening_map:
            ob = opening_map[account_code]
            balance = ob.get('debit', 0) - ob.get('credit', 0)
        
        # Journal entries
        for entry in journal_entries:
            for line in entry.get('lines', []):
                if line['account_code'] == account_code:
                    balance += line.get('debit', 0) - line.get('credit', 0)
        
        # Receipts (debit to cash)
        for receipt in receipts:
            if receipt['account_code'] == account_code:
                balance += receipt['amount']
        
        # Payments (credit to cash)
        for payment in payments:
            if payment['account_code'] == account_code:
                balance -= payment['amount']
        
        return balance if is_debit_normal else -balance
    
    # Assets
    current_assets = []
    non_current_assets = []
    total_current_assets = 0
    total_non_current_assets = 0
    
    for acc in asset_accounts:
        if acc['level'] > 1:
            balance = calculate_balance(acc['account_code'], True)
            if balance != 0:
                item = {
                    'account_code': acc['account_code'],
                    'account_name_ar': acc['account_name_ar'],
                    'account_name_en': acc['account_name_en'],
                    'amount': balance
                }
                if acc['account_code'].startswith('11'):  # Current assets
                    current_assets.append(item)
                    total_current_assets += balance
                else:  # Non-current assets
                    non_current_assets.append(item)
                    total_non_current_assets += balance
    
    total_assets = total_current_assets + total_non_current_assets
    
    # Liabilities
    current_liabilities = []
    non_current_liabilities = []
    total_current_liabilities = 0
    total_non_current_liabilities = 0
    
    for acc in liability_accounts:
        if acc['level'] > 1:
            balance = calculate_balance(acc['account_code'], False)
            if balance != 0:
                item = {
                    'account_code': acc['account_code'],
                    'account_name_ar': acc['account_name_ar'],
                    'account_name_en': acc['account_name_en'],
                    'amount': balance
                }
                if acc['account_code'].startswith('21'):  # Current liabilities
                    current_liabilities.append(item)
                    total_current_liabilities += balance
                else:  # Non-current liabilities
                    non_current_liabilities.append(item)
                    total_non_current_liabilities += balance
    
    total_liabilities = total_current_liabilities + total_non_current_liabilities
    
    # Equity
    equity_items = []
    total_equity = 0
    
    for acc in equity_accounts:
        if acc['level'] > 1:
            balance = calculate_balance(acc['account_code'], False)
            if balance != 0:
                equity_items.append({
                    'account_code': acc['account_code'],
                    'account_name_ar': acc['account_name_ar'],
                    'account_name_en': acc['account_name_en'],
                    'amount': balance
                })
                total_equity += balance
    
    # Add retained earnings (net income from income statement)
    income_statement = await get_income_statement(fiscal_year)
    net_income = income_statement['net_income']
    if net_income != 0:
        equity_items.append({
            'account_code': '32100',
            'account_name_ar': 'أرباح العام الحالي',
            'account_name_en': 'Current Year Profit',
            'amount': net_income
        })
        total_equity += net_income
    
    total_liabilities_equity = total_liabilities + total_equity
    
    return {
        'fiscal_year': fiscal_year,
        'report_date': datetime.now(timezone.utc).isoformat(),
        'assets': {
            'current_assets': {
                'items': current_assets,
                'total': total_current_assets
            },
            'non_current_assets': {
                'items': non_current_assets,
                'total': total_non_current_assets
            },
            'total': total_assets
        },
        'liabilities': {
            'current_liabilities': {
                'items': current_liabilities,
                'total': total_current_liabilities
            },
            'non_current_liabilities': {
                'items': non_current_liabilities,
                'total': total_non_current_liabilities
            },
            'total': total_liabilities
        },
        'equity': {
            'items': equity_items,
            'total': total_equity
        },
        'total_liabilities_equity': total_liabilities_equity,
        'is_balanced': abs(total_assets - total_liabilities_equity) < 0.01
    }


@api_router.get("/reports/cash-flow")
async def get_cash_flow_statement(fiscal_year: str = "2025"):
    """Generate Cash Flow Statement (قائمة التدفقات النقدية)"""
    
    # Get cash accounts (11110 - الصندوق, 11120 - البنك)
    cash_codes = ['11110', '11120', '11130', '11140']
    
    # Get opening balances
    opening_balances = await db.opening_balances.find(
        {"fiscal_year": fiscal_year, "account_code": {"$in": cash_codes}}, {"_id": 0}
    ).to_list(100)
    
    opening_cash = sum(ob.get('debit', 0) - ob.get('credit', 0) for ob in opening_balances)
    
    # Get all transactions
    journal_entries = await db.journal_entries.find({}, {"_id": 0}).to_list(10000)
    receipts = await db.receipt_vouchers.find({}, {"_id": 0}).to_list(10000)
    payments = await db.payment_vouchers.find({}, {"_id": 0}).to_list(10000)
    
    # Operating Activities
    operating_receipts = []
    operating_payments = []
    
    # Investing Activities
    investing_receipts = []
    investing_payments = []
    
    # Financing Activities
    financing_receipts = []
    financing_payments = []
    
    # Analyze journal entries for cash movements
    for entry in journal_entries:
        cash_debit = 0
        cash_credit = 0
        other_account = None
        description = entry.get('description', '')
        
        for line in entry.get('lines', []):
            if line['account_code'] in cash_codes:
                cash_debit += line.get('debit', 0)
                cash_credit += line.get('credit', 0)
            else:
                other_account = line['account_code']
        
        if cash_debit > 0:
            # Cash received
            item = {
                'date': entry['entry_date'],
                'description': description or 'تحصيل نقدي',
                'amount': cash_debit
            }
            # Categorize based on other account
            if other_account and other_account.startswith('4'):  # Revenue
                operating_receipts.append(item)
            elif other_account and other_account.startswith('12'):  # Non-current assets
                investing_receipts.append(item)
            elif other_account and (other_account.startswith('22') or other_account.startswith('3')):  # Long-term liab or equity
                financing_receipts.append(item)
            else:
                operating_receipts.append(item)
        
        if cash_credit > 0:
            # Cash paid
            item = {
                'date': entry['entry_date'],
                'description': description or 'صرف نقدي',
                'amount': cash_credit
            }
            if other_account and other_account.startswith('5'):  # Expenses
                operating_payments.append(item)
            elif other_account and other_account.startswith('12'):  # Non-current assets
                investing_payments.append(item)
            elif other_account and (other_account.startswith('22') or other_account.startswith('3')):  # Long-term liab or equity
                financing_payments.append(item)
            else:
                operating_payments.append(item)
    
    # Receipt vouchers (cash received)
    for receipt in receipts:
        operating_receipts.append({
            'date': receipt['voucher_date'],
            'description': receipt.get('description', 'سند قبض'),
            'amount': receipt['amount']
        })
    
    # Payment vouchers (cash paid)
    for payment in payments:
        operating_payments.append({
            'date': payment['voucher_date'],
            'description': payment.get('description', 'سند صرف'),
            'amount': payment['amount']
        })
    
    # Calculate totals
    total_operating_receipts = sum(item['amount'] for item in operating_receipts)
    total_operating_payments = sum(item['amount'] for item in operating_payments)
    net_operating = total_operating_receipts - total_operating_payments
    
    total_investing_receipts = sum(item['amount'] for item in investing_receipts)
    total_investing_payments = sum(item['amount'] for item in investing_payments)
    net_investing = total_investing_receipts - total_investing_payments
    
    total_financing_receipts = sum(item['amount'] for item in financing_receipts)
    total_financing_payments = sum(item['amount'] for item in financing_payments)
    net_financing = total_financing_receipts - total_financing_payments
    
    net_change = net_operating + net_investing + net_financing
    ending_cash = opening_cash + net_change
    
    return {
        'fiscal_year': fiscal_year,
        'report_date': datetime.now(timezone.utc).isoformat(),
        'opening_cash': opening_cash,
        'operating_activities': {
            'receipts': operating_receipts,
            'total_receipts': total_operating_receipts,
            'payments': operating_payments,
            'total_payments': total_operating_payments,
            'net': net_operating
        },
        'investing_activities': {
            'receipts': investing_receipts,
            'total_receipts': total_investing_receipts,
            'payments': investing_payments,
            'total_payments': total_investing_payments,
            'net': net_investing
        },
        'financing_activities': {
            'receipts': financing_receipts,
            'total_receipts': total_financing_receipts,
            'payments': financing_payments,
            'total_payments': total_financing_payments,
            'net': net_financing
        },
        'net_change': net_change,
        'ending_cash': ending_cash
    }


# ==================== Backup & Restore ====================

@api_router.get("/backup/export")
async def export_backup():
    """Export all data as JSON backup"""
    try:
        backup_data = {
            'backup_date': datetime.now(timezone.utc).isoformat(),
            'version': '1.0',
            'data': {}
        }
        
        # Export all collections
        collections = [
            'users', 'chart_of_accounts', 'opening_balances', 
            'customers', 'invoices', 'journal_entries', 
            'receipt_vouchers', 'payment_vouchers'
        ]
        
        for collection in collections:
            data = await db[collection].find({}, {"_id": 0}).to_list(100000)
            backup_data['data'][collection] = data
        
        # Convert to JSON
        json_data = json.dumps(backup_data, ensure_ascii=False, indent=2, default=str)
        
        # Create BytesIO buffer
        buffer = BytesIO(json_data.encode('utf-8'))
        buffer.seek(0)
        
        filename = f"backup_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
        
        return StreamingResponse(
            buffer,
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Backup failed: {str(e)}")

@api_router.post("/backup/import")
async def import_backup(file: UploadFile = File(...)):
    """Import data from JSON backup"""
    try:
        # Read uploaded file
        content = await file.read()
        backup_data = json.loads(content.decode('utf-8'))
        
        if 'data' not in backup_data:
            raise HTTPException(status_code=400, detail="Invalid backup file format")
        
        # Clear existing data (optional - be careful!)
        # Commented out for safety
        # collections = ['chart_of_accounts', 'opening_balances', 'customers', 'invoices', 
        #                'journal_entries', 'receipt_vouchers', 'payment_vouchers']
        # for collection in collections:
        #     await db[collection].delete_many({})
        
        # Import data
        imported_counts = {}
        for collection, data in backup_data['data'].items():
            if collection == 'users':
                continue  # Skip users for security
            
            if data:
                # Delete existing data for this collection
                await db[collection].delete_many({})
                # Insert new data
                await db[collection].insert_many(data)
                imported_counts[collection] = len(data)
        
        return {
            "message": "Backup imported successfully",
            "imported_counts": imported_counts,
            "backup_date": backup_data.get('backup_date')
        }
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON file")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")

# ==================== User Management ====================

class UserCreate(BaseModel):
    username: str
    password: str
    full_name: str

class UserResponse(BaseModel):
    username: str
    full_name: str
    created_at: str

class CompanySettings(BaseModel):
    model_config = ConfigDict(extra="ignore")
    company_name_ar: str
    company_name_en: str
    commercial_registration: Optional[str] = None
    tax_number: Optional[str] = None
    address_ar: Optional[str] = None
    address_en: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    logo_url: Optional[str] = None
    logo_base64: Optional[str] = None
    primary_color: Optional[str] = "#006d5b"
    invoice_color: Optional[str] = "#006d5b"
    voucher_color: Optional[str] = "#006d5b"
    invoice_template: Optional[str] = "classic"
    voucher_template: Optional[str] = "classic"
    license_expiry: Optional[str] = None
    app_version: Optional[str] = "1.0.0"
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

@api_router.get("/users", response_model=List[UserResponse])
async def get_users():
    """Get all users (excluding passwords)"""
    users = await db.users.find({}, {"_id": 0, "password_hash": 0}).to_list(1000)
    for user in users:
        if isinstance(user.get('created_at'), str):
            pass
        else:
            user['created_at'] = user.get('created_at', datetime.now(timezone.utc)).isoformat()
    return users

@api_router.post("/users", response_model=UserResponse)
async def create_user(user: UserCreate):
    """Create a new user"""
    existing = await db.users.find_one({"username": user.username})
    if existing:
        raise HTTPException(status_code=400, detail="Username already exists")
    
    new_user = User(
        username=user.username,
        password_hash=pwd_context.hash(user.password),
        full_name=user.full_name
    )
    
    doc = new_user.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    await db.users.insert_one(doc)
    
    return UserResponse(
        username=new_user.username,
        full_name=new_user.full_name,
        created_at=doc['created_at']
    )

@api_router.delete("/users/{username}")
async def delete_user(username: str):
    """Delete a user"""
    # Prevent deleting the last user or admin
    total_users = await db.users.count_documents({})
    if total_users <= 1:
        raise HTTPException(status_code=400, detail="Cannot delete the last user")
    
    if username == "admin":
        raise HTTPException(status_code=400, detail="Cannot delete admin user")
    
    result = await db.users.delete_one({"username": username})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {"message": "User deleted successfully"}

@api_router.put("/users/{username}/password")
async def change_password(username: str, data: dict):
    """Change user password"""
    new_password = data.get('new_password')
    if not new_password or len(new_password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
    
    result = await db.users.update_one(
        {"username": username},
        {"$set": {"password_hash": pwd_context.hash(new_password)}}
    )
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {"message": "Password changed successfully"}

# ==================== Company Settings ====================

@api_router.get("/company-settings")
async def get_company_settings(x_tenant_id: str = Header(None)):
    """Get company settings - supports both main app and tenant mode"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    
    settings = await target_db.company_settings.find_one({}, {"_id": 0})
    if not settings:
        # Return default settings
        return {
            "company_name_ar": "اسم الشركة",
            "company_name_en": "Company Name",
            "commercial_registration": "",
            "tax_number": "",
            "address_ar": "",
            "address_en": "",
            "phone": "",
            "email": "",
            "logo_url": "",
            "logo_base64": "",
            "primary_color": "#006d5b",
            "invoice_color": "#006d5b",
            "voucher_color": "#006d5b",
            "invoice_template": "classic",
            "voucher_template": "classic",
            "license_expiry": "",
            "app_version": "1.0.0"
        }
    if isinstance(settings.get('updated_at'), str):
        pass
    else:
        settings['updated_at'] = settings.get('updated_at', datetime.now(timezone.utc)).isoformat()
    # Ensure fields have defaults
    settings.setdefault('invoice_color', '#006d5b')
    settings.setdefault('voucher_color', '#006d5b')
    settings.setdefault('primary_color', '#006d5b')
    settings.setdefault('logo_base64', '')
    settings.setdefault('invoice_template', 'classic')
    settings.setdefault('voucher_template', 'classic')
    settings.setdefault('license_expiry', '')
    settings.setdefault('app_version', '1.0.0')
    return settings

@api_router.post("/company-settings")
async def update_company_settings(settings: CompanySettings, x_tenant_id: str = Header(None)):
    """Update company settings - supports both main app and tenant mode"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    
    doc = settings.model_dump()
    doc['updated_at'] = doc['updated_at'].isoformat()
    
    # Delete all existing settings and insert new one
    await target_db.company_settings.delete_many({})
    await target_db.company_settings.insert_one(doc)
    
    return {"message": "Company settings updated successfully"}

# ==================== Import Chart of Accounts from Excel ====================

@api_router.post("/accounts/import-excel")
async def import_accounts_from_excel(file: UploadFile = File(...)):
    """Import chart of accounts from Excel file"""
    try:
        import openpyxl
        from io import BytesIO
        
        # Read uploaded file
        content = await file.read()
        workbook = openpyxl.load_workbook(BytesIO(content))
        sheet = workbook.active
        
        imported_count = 0
        skipped_count = 0
        
        # Assuming columns: account_code, account_name_ar, account_name_en, account_type, parent_code, level
        for row in sheet.iter_rows(min_row=2, values_only=True):  # Skip header row
            if not row[0]:  # Skip empty rows
                continue
                
            account_code = str(row[0]).strip()
            account_name_ar = str(row[1]).strip() if row[1] else ""
            account_name_en = str(row[2]).strip() if row[2] else ""
            account_type = str(row[3]).strip().lower() if row[3] else "asset"
            parent_code = str(row[4]).strip() if row[4] and str(row[4]).strip() else None
            level = int(row[5]) if row[5] else 1
            
            # Check if account already exists
            existing = await db.chart_of_accounts.find_one({"account_code": account_code})
            if existing:
                skipped_count += 1
                continue
            
            # Create account
            account = ChartOfAccount(
                account_code=account_code,
                account_name_ar=account_name_ar,
                account_name_en=account_name_en,
                account_type=account_type,
                parent_code=parent_code,
                level=level,
                is_active=True
            )
            
            doc = account.model_dump()
            doc['created_at'] = doc['created_at'].isoformat()
            await db.chart_of_accounts.insert_one(doc)
            imported_count += 1
        
        return {
            "message": "Import completed",
            "imported": imported_count,
            "skipped": skipped_count,
            "total": imported_count + skipped_count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")

# ==================== Cost Centers ====================

@api_router.get("/cost-centers")
async def get_cost_centers(x_tenant_id: str = Header(None)):
    """Get all cost centers"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    centers = await target_db.cost_centers.find({}, {"_id": 0}).sort("center_code", 1).to_list(500)
    return centers

@api_router.post("/cost-centers")
async def create_cost_center(center: CostCenter, x_tenant_id: str = Header(None)):
    """Create a new cost center"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    
    existing = await target_db.cost_centers.find_one({"center_code": center.center_code})
    if existing:
        raise HTTPException(status_code=400, detail="رمز مركز التكلفة موجود مسبقاً")
    
    doc = center.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    await target_db.cost_centers.insert_one(doc)
    return {"message": "تم إنشاء مركز التكلفة بنجاح", "center_code": center.center_code}

@api_router.put("/cost-centers/{center_code}")
async def update_cost_center(center_code: str, center: CostCenter, x_tenant_id: str = Header(None)):
    """Update a cost center"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    
    doc = center.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    del doc['center_code']  # Don't update the code
    
    result = await target_db.cost_centers.update_one(
        {"center_code": center_code},
        {"$set": doc}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="مركز التكلفة غير موجود")
    return {"message": "تم تحديث مركز التكلفة بنجاح"}

@api_router.delete("/cost-centers/{center_code}")
async def delete_cost_center(center_code: str, x_tenant_id: str = Header(None)):
    """Delete a cost center"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    result = await target_db.cost_centers.delete_one({"center_code": center_code})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="مركز التكلفة غير موجود")
    return {"message": "تم حذف مركز التكلفة بنجاح"}

# ==================== Projects ====================

@api_router.get("/projects")
async def get_projects(x_tenant_id: str = Header(None)):
    """Get all projects"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    projects = await target_db.projects.find({}, {"_id": 0}).sort("project_code", 1).to_list(500)
    return projects

@api_router.post("/projects")
async def create_project(project: Project, x_tenant_id: str = Header(None)):
    """Create a new project"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    
    existing = await target_db.projects.find_one({"project_code": project.project_code})
    if existing:
        raise HTTPException(status_code=400, detail="رمز المشروع موجود مسبقاً")
    
    doc = project.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    await target_db.projects.insert_one(doc)
    return {"message": "تم إنشاء المشروع بنجاح", "project_code": project.project_code}

@api_router.put("/projects/{project_code}")
async def update_project(project_code: str, project: Project, x_tenant_id: str = Header(None)):
    """Update a project"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    
    doc = project.model_dump()
    doc['created_at'] = doc['created_at'].isoformat()
    del doc['project_code']  # Don't update the code
    
    result = await target_db.projects.update_one(
        {"project_code": project_code},
        {"$set": doc}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="المشروع غير موجود")
    return {"message": "تم تحديث المشروع بنجاح"}

@api_router.delete("/projects/{project_code}")
async def delete_project(project_code: str, x_tenant_id: str = Header(None)):
    """Delete a project"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    result = await target_db.projects.delete_one({"project_code": project_code})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="المشروع غير موجود")
    return {"message": "تم حذف المشروع بنجاح"}

# ==================== License Expiry Notifications ====================

@api_router.get("/super-admin/license-alerts")
async def get_license_alerts():
    """Get tenants with expiring or expired licenses"""
    from datetime import timedelta
    
    today = datetime.now(timezone.utc).date()
    alerts = []
    
    tenants = await db.tenants.find({}, {"_id": 0}).to_list(500)
    
    for tenant in tenants:
        try:
            expiry_str = tenant.get('license_expiry', '')
            if not expiry_str:
                continue
            
            # Parse date (handle different formats)
            if 'T' in expiry_str:
                expiry_date = datetime.fromisoformat(expiry_str.replace('Z', '+00:00')).date()
            else:
                expiry_date = datetime.strptime(expiry_str[:10], '%Y-%m-%d').date()
            
            days_remaining = (expiry_date - today).days
            
            alert = {
                "tenant_id": tenant['tenant_id'],
                "company_name_ar": tenant.get('company_name_ar', ''),
                "company_name_en": tenant.get('company_name_en', ''),
                "license_expiry": expiry_str[:10],
                "days_remaining": days_remaining,
                "is_active": tenant.get('is_active', True)
            }
            
            if days_remaining < 0:
                alert['status'] = 'expired'
                alert['severity'] = 'critical'
                alert['message_ar'] = f'انتهى الترخيص منذ {abs(days_remaining)} يوم'
                alerts.append(alert)
            elif days_remaining <= 7:
                alert['status'] = 'expiring_soon'
                alert['severity'] = 'high'
                alert['message_ar'] = f'ينتهي الترخيص خلال {days_remaining} أيام'
                alerts.append(alert)
            elif days_remaining <= 30:
                alert['status'] = 'expiring'
                alert['severity'] = 'medium'
                alert['message_ar'] = f'ينتهي الترخيص خلال {days_remaining} يوم'
                alerts.append(alert)
        except Exception as e:
            continue
    
    # Sort by days remaining (expired first, then closest to expiry)
    alerts.sort(key=lambda x: x['days_remaining'])
    
    return {
        "total_alerts": len(alerts),
        "critical": len([a for a in alerts if a['severity'] == 'critical']),
        "high": len([a for a in alerts if a['severity'] == 'high']),
        "medium": len([a for a in alerts if a['severity'] == 'medium']),
        "alerts": alerts
    }

# ==================== Reports by Cost Center / Project ====================

@api_router.get("/reports/by-cost-center")
async def get_report_by_cost_center(x_tenant_id: str = Header(None)):
    """Get financial summary by cost center"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    
    # Get all cost centers
    centers = await target_db.cost_centers.find({}, {"_id": 0}).to_list(500)
    
    # Aggregate journal entries by cost center
    report = []
    for center in centers:
        pipeline = [
            {"$unwind": "$lines"},
            {"$match": {"lines.cost_center_code": center['center_code']}},
            {"$group": {
                "_id": None,
                "total_debit": {"$sum": "$lines.debit"},
                "total_credit": {"$sum": "$lines.credit"},
                "entries_count": {"$sum": 1}
            }}
        ]
        
        result = await target_db.journal_entries.aggregate(pipeline).to_list(1)
        
        if result:
            report.append({
                "center_code": center['center_code'],
                "center_name_ar": center['center_name_ar'],
                "center_name_en": center.get('center_name_en', ''),
                "total_debit": result[0]['total_debit'],
                "total_credit": result[0]['total_credit'],
                "net_amount": result[0]['total_debit'] - result[0]['total_credit'],
                "entries_count": result[0]['entries_count']
            })
        else:
            report.append({
                "center_code": center['center_code'],
                "center_name_ar": center['center_name_ar'],
                "center_name_en": center.get('center_name_en', ''),
                "total_debit": 0,
                "total_credit": 0,
                "net_amount": 0,
                "entries_count": 0
            })
    
    return report

@api_router.get("/reports/by-project")
async def get_report_by_project(x_tenant_id: str = Header(None)):
    """Get financial summary by project"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    
    # Get all projects
    projects = await target_db.projects.find({}, {"_id": 0}).to_list(500)
    
    # Aggregate journal entries by project
    report = []
    for project in projects:
        pipeline = [
            {"$unwind": "$lines"},
            {"$match": {"lines.project_code": project['project_code']}},
            {"$group": {
                "_id": None,
                "total_debit": {"$sum": "$lines.debit"},
                "total_credit": {"$sum": "$lines.credit"},
                "entries_count": {"$sum": 1}
            }}
        ]
        
        result = await target_db.journal_entries.aggregate(pipeline).to_list(1)
        
        budget = project.get('budget', 0)
        spent = 0
        if result:
            spent = result[0]['total_debit']
        
        report.append({
            "project_code": project['project_code'],
            "project_name_ar": project['project_name_ar'],
            "project_name_en": project.get('project_name_en', ''),
            "status": project.get('status', 'active'),
            "budget": budget,
            "total_debit": result[0]['total_debit'] if result else 0,
            "total_credit": result[0]['total_credit'] if result else 0,
            "net_amount": (result[0]['total_debit'] - result[0]['total_credit']) if result else 0,
            "entries_count": result[0]['entries_count'] if result else 0,
            "budget_used_percent": round((spent / budget * 100), 2) if budget > 0 else 0
        })
    
    return report

# ==================== Quarterly Reports ====================

@api_router.get("/reports/quarterly")
async def get_quarterly_report(year: int, quarter: int, x_tenant_id: str = Header(None)):
    """Get quarterly financial report with revenues and expenses"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    
    # Calculate quarter date range
    quarter_months = {
        1: (1, 3),   # Q1: Jan-Mar
        2: (4, 6),   # Q2: Apr-Jun
        3: (7, 9),   # Q3: Jul-Sep
        4: (10, 12)  # Q4: Oct-Dec
    }
    
    if quarter not in quarter_months:
        raise HTTPException(status_code=400, detail="Invalid quarter (1-4)")
    
    start_month, end_month = quarter_months[quarter]
    start_date = f"{year}-{start_month:02d}-01"
    end_date = f"{year}-{end_month:02d}-31"
    
    # Get previous quarter's end date for carried forward balance
    if quarter == 1:
        prev_end_date = f"{year-1}-12-31"
    else:
        prev_start_month, prev_end_month = quarter_months[quarter - 1]
        prev_end_date = f"{year}-{prev_end_month:02d}-31"
    
    # Get all revenue accounts (type = 'revenue')
    revenue_accounts = await target_db.chart_of_accounts.find(
        {"account_type": "revenue"},
        {"account_code": 1, "account_name_ar": 1, "_id": 0}
    ).to_list(500)
    revenue_codes = [acc['account_code'] for acc in revenue_accounts]
    
    # Get all expense accounts (type = 'expense')
    expense_accounts = await target_db.chart_of_accounts.find(
        {"account_type": "expense"},
        {"account_code": 1, "account_name_ar": 1, "_id": 0}
    ).to_list(500)
    expense_codes = [acc['account_code'] for acc in expense_accounts]
    
    # Get bank/cash accounts for available balance
    bank_accounts = await target_db.chart_of_accounts.find(
        {"$or": [
            {"account_name_ar": {"$regex": "بنك|صندوق|نقد", "$options": "i"}},
            {"account_code": {"$regex": "^111"}}
        ]},
        {"account_code": 1, "account_name_ar": 1, "_id": 0}
    ).to_list(50)
    bank_codes = [acc['account_code'] for acc in bank_accounts]
    
    # Calculate carried forward balance (previous period closing)
    carried_forward_revenue = 0
    carried_forward_expense = 0
    
    prev_entries = await target_db.journal_entries.find({
        "entry_date": {"$lt": start_date}
    }).to_list(5000)
    
    for entry in prev_entries:
        for line in entry.get('lines', []):
            if line['account_code'] in revenue_codes:
                carried_forward_revenue += line.get('credit', 0) - line.get('debit', 0)
            elif line['account_code'] in expense_codes:
                carried_forward_expense += line.get('debit', 0) - line.get('credit', 0)
    
    # Calculate current quarter movements
    quarter_entries = await target_db.journal_entries.find({
        "entry_date": {"$gte": start_date, "$lte": end_date}
    }).to_list(5000)
    
    # Monthly breakdown for charts
    monthly_data = {}
    for m in range(start_month, end_month + 1):
        monthly_data[m] = {"month": m, "revenue": 0, "expense": 0}
    
    current_revenue = 0
    current_expense = 0
    revenue_by_account = {}
    expense_by_account = {}
    
    for entry in quarter_entries:
        entry_date = str(entry.get('entry_date', ''))[:10]
        try:
            entry_month = int(entry_date[5:7])
        except:
            entry_month = start_month
        
        for line in entry.get('lines', []):
            acc_code = line['account_code']
            debit = line.get('debit', 0)
            credit = line.get('credit', 0)
            
            if acc_code in revenue_codes:
                amount = credit - debit
                current_revenue += amount
                if entry_month in monthly_data:
                    monthly_data[entry_month]['revenue'] += amount
                # Track by account
                if acc_code not in revenue_by_account:
                    acc_info = next((a for a in revenue_accounts if a['account_code'] == acc_code), {})
                    revenue_by_account[acc_code] = {
                        "account_code": acc_code,
                        "account_name": acc_info.get('account_name_ar', acc_code),
                        "amount": 0
                    }
                revenue_by_account[acc_code]['amount'] += amount
                
            elif acc_code in expense_codes:
                amount = debit - credit
                current_expense += amount
                if entry_month in monthly_data:
                    monthly_data[entry_month]['expense'] += amount
                # Track by account
                if acc_code not in expense_by_account:
                    acc_info = next((a for a in expense_accounts if a['account_code'] == acc_code), {})
                    expense_by_account[acc_code] = {
                        "account_code": acc_code,
                        "account_name": acc_info.get('account_name_ar', acc_code),
                        "amount": 0
                    }
                expense_by_account[acc_code]['amount'] += amount
    
    # Calculate bank balance as of today
    all_entries = await target_db.journal_entries.find({}).to_list(10000)
    bank_balance = 0
    for entry in all_entries:
        for line in entry.get('lines', []):
            if line['account_code'] in bank_codes:
                bank_balance += line.get('debit', 0) - line.get('credit', 0)
    
    # Add opening balances for bank accounts
    bank_opening = await target_db.opening_balances.find(
        {"account_code": {"$in": bank_codes}}
    ).to_list(100)
    for ob in bank_opening:
        bank_balance += ob.get('debit', 0) - ob.get('credit', 0)
    
    # Prepare chart data
    chart_data = []
    month_names = {
        1: "يناير", 2: "فبراير", 3: "مارس", 4: "أبريل",
        5: "مايو", 6: "يونيو", 7: "يوليو", 8: "أغسطس",
        9: "سبتمبر", 10: "أكتوبر", 11: "نوفمبر", 12: "ديسمبر"
    }
    for m in range(start_month, end_month + 1):
        chart_data.append({
            "month": month_names.get(m, str(m)),
            "revenue": round(monthly_data[m]['revenue'], 2),
            "expense": round(monthly_data[m]['expense'], 2),
            "net": round(monthly_data[m]['revenue'] - monthly_data[m]['expense'], 2)
        })
    
    # Sort top accounts
    top_revenue = sorted(revenue_by_account.values(), key=lambda x: x['amount'], reverse=True)[:10]
    top_expense = sorted(expense_by_account.values(), key=lambda x: x['amount'], reverse=True)[:10]
    
    return {
        "period": {
            "year": year,
            "quarter": quarter,
            "quarter_name": f"Q{quarter}",
            "start_date": start_date,
            "end_date": end_date
        },
        "summary": {
            "carried_forward": {
                "revenue": round(carried_forward_revenue, 2),
                "expense": round(carried_forward_expense, 2),
                "net": round(carried_forward_revenue - carried_forward_expense, 2)
            },
            "current_period": {
                "revenue": round(current_revenue, 2),
                "expense": round(current_expense, 2),
                "net": round(current_revenue - current_expense, 2)
            },
            "closing_balance": {
                "revenue": round(carried_forward_revenue + current_revenue, 2),
                "expense": round(carried_forward_expense + current_expense, 2),
                "net": round((carried_forward_revenue + current_revenue) - (carried_forward_expense + current_expense), 2)
            },
            "bank_balance": round(bank_balance, 2)
        },
        "chart_data": chart_data,
        "top_revenue_accounts": top_revenue,
        "top_expense_accounts": top_expense,
        "totals": {
            "total_revenue": round(current_revenue, 2),
            "total_expense": round(current_expense, 2),
            "net_profit": round(current_revenue - current_expense, 2),
            "profit_margin": round((current_revenue - current_expense) / current_revenue * 100, 2) if current_revenue > 0 else 0
        }
    }

@api_router.post("/reports/quarterly/ai-analysis")
async def get_quarterly_ai_analysis(request: dict, x_tenant_id: str = Header(None)):
    """Get AI-powered analysis and recommendations for quarterly report"""
    from emergentintegrations.llm.chat import LlmChat, UserMessage
    
    report_data = request.get('report_data', {})
    
    # Prepare financial data summary for AI
    summary = report_data.get('summary', {})
    totals = report_data.get('totals', {})
    period = report_data.get('period', {})
    top_expenses = report_data.get('top_expense_accounts', [])
    top_revenues = report_data.get('top_revenue_accounts', [])
    
    # Build context for AI
    expense_list = "\n".join([f"- {e['account_name']}: {e['amount']:,.2f} ر.س" for e in top_expenses[:5]])
    revenue_list = "\n".join([f"- {r['account_name']}: {r['amount']:,.2f} ر.س" for r in top_revenues[:5]])
    
    financial_context = f"""
بيانات التقرير الربعي للربع {period.get('quarter', '')} من عام {period.get('year', '')}:

📊 ملخص الأرقام:
- إجمالي الإيرادات: {totals.get('total_revenue', 0):,.2f} ر.س
- إجمالي المصروفات: {totals.get('total_expense', 0):,.2f} ر.س
- صافي الربح/الخسارة: {totals.get('net_profit', 0):,.2f} ر.س
- هامش الربح: {totals.get('profit_margin', 0):.1f}%
- الرصيد المتاح في البنك: {summary.get('bank_balance', 0):,.2f} ر.س

📈 الرصيد المرحل من الفترة السابقة:
- إيرادات مرحلة: {summary.get('carried_forward', {}).get('revenue', 0):,.2f} ر.س
- مصروفات مرحلة: {summary.get('carried_forward', {}).get('expense', 0):,.2f} ر.س

💰 أعلى مصادر الإيرادات:
{revenue_list}

💸 أعلى بنود المصروفات:
{expense_list}
"""

    try:
        api_key = os.environ.get('EMERGENT_LLM_KEY')
        if not api_key:
            raise HTTPException(status_code=500, detail="AI service not configured")
        
        chat = LlmChat(
            api_key=api_key,
            session_id=f"quarterly-analysis-{period.get('year')}-{period.get('quarter')}",
            system_message="""أنت محلل مالي خبير ومستشار أعمال محترف. مهمتك تحليل البيانات المالية الربعية وتقديم:
1. تحليل شامل للوضع المالي
2. نقاط القوة والضعف
3. توصيات عملية لتحسين الأداء
4. تحذيرات من المخاطر المحتملة
5. فرص النمو والتطوير

استخدم أسلوباً احترافياً وإبداعياً مع إيموجي مناسبة. قدم رؤى ذكية وقابلة للتنفيذ.
اكتب بالعربية فقط."""
        ).with_model("openai", "gpt-4o")
        
        user_message = UserMessage(
            text=f"""قم بتحليل هذا التقرير المالي الربعي وقدم:
1. 📊 تحليل الأداء المالي (3-4 نقاط)
2. ✅ نقاط القوة (2-3 نقاط)
3. ⚠️ نقاط تحتاج تحسين (2-3 نقاط)
4. 💡 توصيات استراتيجية (3-4 توصيات عملية)
5. 🎯 أهداف مقترحة للربع القادم

{financial_context}"""
        )
        
        response = await chat.send_message(user_message)
        
        return {
            "analysis": response,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "period": period
        }
        
    except Exception as e:
        logger.error(f"AI Analysis error: {str(e)}")
        # Fallback analysis if AI fails
        net_profit = totals.get('net_profit', 0)
        profit_margin = totals.get('profit_margin', 0)
        bank_balance = summary.get('bank_balance', 0)
        
        if net_profit > 0:
            profit_status = "إيجابي ✅"
            recommendation = "استمر في هذا الاتجاه مع التركيز على تقليل المصروفات غير الضرورية"
        else:
            profit_status = "سلبي ⚠️"
            recommendation = "يجب مراجعة المصروفات وزيادة مصادر الإيرادات"
        
        fallback_analysis = f"""
## 📊 تحليل الأداء المالي

### الوضع العام
- صافي الربح للربع: **{net_profit:,.2f} ر.س** ({profit_status})
- هامش الربح: **{profit_margin:.1f}%**
- الرصيد البنكي المتاح: **{bank_balance:,.2f} ر.س**

### 💡 التوصيات
1. {recommendation}
2. راقب التدفقات النقدية بشكل دوري
3. قارن الأداء مع الأرباع السابقة

*ملاحظة: هذا تحليل أساسي. للحصول على تحليل متقدم، تأكد من تفعيل خدمة الذكاء الاصطناعي.*
"""
        
        return {
            "analysis": fallback_analysis,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "period": period,
            "is_fallback": True
        }

# ==================== Historical Data Management ====================

class HistoricalYearData(BaseModel):
    """Model for entering previous year's financial summary"""
    model_config = ConfigDict(extra="ignore")
    year: int
    total_revenue: float = 0.0
    total_expense: float = 0.0
    net_profit: float = 0.0
    closing_cash_balance: float = 0.0
    closing_bank_balance: float = 0.0
    notes: str = ""
    q1_revenue: float = 0.0
    q1_expense: float = 0.0
    q2_revenue: float = 0.0
    q2_expense: float = 0.0
    q3_revenue: float = 0.0
    q3_expense: float = 0.0
    q4_revenue: float = 0.0
    q4_expense: float = 0.0

@api_router.get("/reports/quarterly/comparison")
async def get_quarterly_comparison(year: int, quarter: int, x_tenant_id: str = Header(None)):
    """Get comparison data between current quarter and previous quarters"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    
    comparison_data = []
    
    # Get data for last 4 quarters (including current)
    quarters_to_fetch = []
    for i in range(4):
        q = quarter - i
        y = year
        while q <= 0:
            q += 4
            y -= 1
        quarters_to_fetch.append({"year": y, "quarter": q})
    
    quarters_to_fetch.reverse()  # Chronological order
    
    for period in quarters_to_fetch:
        y, q = period['year'], period['quarter']
        
        # First check if we have historical data for this period
        historical = await target_db.historical_year_data.find_one({"year": y})
        
        quarter_months = {1: (1, 3), 2: (4, 6), 3: (7, 9), 4: (10, 12)}
        start_month, end_month = quarter_months[q]
        start_date = f"{y}-{start_month:02d}-01"
        end_date = f"{y}-{end_month:02d}-31"
        
        # Get revenue and expense accounts
        revenue_codes = [acc['account_code'] for acc in await target_db.chart_of_accounts.find(
            {"account_type": "revenue"}, {"account_code": 1, "_id": 0}
        ).to_list(500)]
        
        expense_codes = [acc['account_code'] for acc in await target_db.chart_of_accounts.find(
            {"account_type": "expense"}, {"account_code": 1, "_id": 0}
        ).to_list(500)]
        
        # Calculate from journal entries
        entries = await target_db.journal_entries.find({
            "entry_date": {"$gte": start_date, "$lte": end_date}
        }).to_list(5000)
        
        revenue = 0
        expense = 0
        
        for entry in entries:
            for line in entry.get('lines', []):
                if line['account_code'] in revenue_codes:
                    revenue += line.get('credit', 0) - line.get('debit', 0)
                elif line['account_code'] in expense_codes:
                    expense += line.get('debit', 0) - line.get('credit', 0)
        
        # If no journal entries, check historical data
        if revenue == 0 and expense == 0 and historical:
            quarter_field_map = {
                1: ('q1_revenue', 'q1_expense'),
                2: ('q2_revenue', 'q2_expense'),
                3: ('q3_revenue', 'q3_expense'),
                4: ('q4_revenue', 'q4_expense')
            }
            rev_field, exp_field = quarter_field_map[q]
            revenue = historical.get(rev_field, 0)
            expense = historical.get(exp_field, 0)
        
        comparison_data.append({
            "year": y,
            "quarter": q,
            "label": f"Q{q} {y}",
            "revenue": round(revenue, 2),
            "expense": round(expense, 2),
            "net_profit": round(revenue - expense, 2),
            "is_current": (y == year and q == quarter),
            "has_data": revenue > 0 or expense > 0
        })
    
    # Calculate growth rates
    if len(comparison_data) >= 2:
        current = comparison_data[-1]
        previous = comparison_data[-2]
        
        if previous['revenue'] > 0:
            current['revenue_growth'] = round(((current['revenue'] - previous['revenue']) / previous['revenue']) * 100, 1)
        else:
            current['revenue_growth'] = 0
            
        if previous['expense'] > 0:
            current['expense_growth'] = round(((current['expense'] - previous['expense']) / previous['expense']) * 100, 1)
        else:
            current['expense_growth'] = 0
    
    return {
        "current_period": {"year": year, "quarter": quarter},
        "comparison": comparison_data
    }

@api_router.get("/reports/yearly-summary")
async def get_yearly_summary(year: int, x_tenant_id: str = Header(None)):
    """Get yearly financial summary"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    
    # Check for historical data first
    historical = await target_db.historical_year_data.find_one({"year": year}, {"_id": 0})
    
    # Calculate from journal entries
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    
    revenue_codes = [acc['account_code'] for acc in await target_db.chart_of_accounts.find(
        {"account_type": "revenue"}, {"account_code": 1, "_id": 0}
    ).to_list(500)]
    
    expense_codes = [acc['account_code'] for acc in await target_db.chart_of_accounts.find(
        {"account_type": "expense"}, {"account_code": 1, "_id": 0}
    ).to_list(500)]
    
    entries = await target_db.journal_entries.find({
        "entry_date": {"$gte": start_date, "$lte": end_date}
    }).to_list(10000)
    
    calculated_revenue = 0
    calculated_expense = 0
    
    for entry in entries:
        for line in entry.get('lines', []):
            if line['account_code'] in revenue_codes:
                calculated_revenue += line.get('credit', 0) - line.get('debit', 0)
            elif line['account_code'] in expense_codes:
                calculated_expense += line.get('debit', 0) - line.get('credit', 0)
    
    return {
        "year": year,
        "calculated": {
            "total_revenue": round(calculated_revenue, 2),
            "total_expense": round(calculated_expense, 2),
            "net_profit": round(calculated_revenue - calculated_expense, 2)
        },
        "historical": historical,
        "has_historical_data": historical is not None
    }

@api_router.get("/historical-data")
async def get_all_historical_data(x_tenant_id: str = Header(None)):
    """Get all historical year data"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    data = await target_db.historical_year_data.find({}, {"_id": 0}).sort("year", -1).to_list(50)
    return data

@api_router.post("/historical-data")
async def save_historical_data(data: HistoricalYearData, x_tenant_id: str = Header(None)):
    """Save or update historical year data"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    
    doc = data.model_dump()
    doc['updated_at'] = datetime.now(timezone.utc).isoformat()
    # Calculate net profit
    doc['net_profit'] = doc['total_revenue'] - doc['total_expense']
    
    # Upsert - update if exists, insert if not
    result = await target_db.historical_year_data.update_one(
        {"year": data.year},
        {"$set": doc},
        upsert=True
    )
    
    return {
        "message": f"تم حفظ بيانات سنة {data.year} بنجاح",
        "year": data.year,
        "updated": result.modified_count > 0,
        "inserted": result.upserted_id is not None
    }

@api_router.delete("/historical-data/{year}")
async def delete_historical_data(year: int, x_tenant_id: str = Header(None)):
    """Delete historical year data"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    result = await target_db.historical_year_data.delete_one({"year": year})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="لم يتم العثور على بيانات هذه السنة")
    return {"message": f"تم حذف بيانات سنة {year} بنجاح"}

# ==================== Quarterly Report Export ====================

@api_router.get("/reports/quarterly/export/pdf")
async def export_quarterly_report_pdf(year: int, quarter: int, x_tenant_id: str = Header(None)):
    """Export quarterly report as PDF"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    
    # Get report data
    # Inline the logic from get_quarterly_report to avoid circular issues
    quarter_months = {1: (1, 3), 2: (4, 6), 3: (7, 9), 4: (10, 12)}
    start_month, end_month = quarter_months.get(quarter, (1, 3))
    start_date = f"{year}-{start_month:02d}-01"
    end_date = f"{year}-{end_month:02d}-31"
    
    # Get accounts
    revenue_accounts = await target_db.chart_of_accounts.find({"account_type": "revenue"}, {"account_code": 1, "account_name_ar": 1, "_id": 0}).to_list(500)
    expense_accounts = await target_db.chart_of_accounts.find({"account_type": "expense"}, {"account_code": 1, "account_name_ar": 1, "_id": 0}).to_list(500)
    revenue_codes = [acc['account_code'] for acc in revenue_accounts]
    expense_codes = [acc['account_code'] for acc in expense_accounts]
    
    # Get entries
    quarter_entries = await target_db.journal_entries.find({"entry_date": {"$gte": start_date, "$lte": end_date}}).to_list(5000)
    prev_entries = await target_db.journal_entries.find({"entry_date": {"$lt": start_date}}).to_list(5000)
    
    # Calculate values
    carried_forward_revenue, carried_forward_expense = 0, 0
    for entry in prev_entries:
        for line in entry.get('lines', []):
            if line['account_code'] in revenue_codes:
                carried_forward_revenue += line.get('credit', 0) - line.get('debit', 0)
            elif line['account_code'] in expense_codes:
                carried_forward_expense += line.get('debit', 0) - line.get('credit', 0)
    
    current_revenue, current_expense = 0, 0
    revenue_by_account, expense_by_account = {}, {}
    monthly_data = {m: {"month": m, "revenue": 0, "expense": 0} for m in range(start_month, end_month + 1)}
    month_names = {1: "يناير", 2: "فبراير", 3: "مارس", 4: "أبريل", 5: "مايو", 6: "يونيو", 7: "يوليو", 8: "أغسطس", 9: "سبتمبر", 10: "أكتوبر", 11: "نوفمبر", 12: "ديسمبر"}
    
    for entry in quarter_entries:
        entry_date = str(entry.get('entry_date', ''))[:10]
        try:
            entry_month = int(entry_date[5:7])
        except:
            entry_month = start_month
        
        for line in entry.get('lines', []):
            acc_code = line['account_code']
            if acc_code in revenue_codes:
                amount = line.get('credit', 0) - line.get('debit', 0)
                current_revenue += amount
                if entry_month in monthly_data:
                    monthly_data[entry_month]['revenue'] += amount
                if acc_code not in revenue_by_account:
                    acc_info = next((a for a in revenue_accounts if a['account_code'] == acc_code), {})
                    revenue_by_account[acc_code] = {"account_code": acc_code, "account_name": acc_info.get('account_name_ar', acc_code), "amount": 0}
                revenue_by_account[acc_code]['amount'] += amount
            elif acc_code in expense_codes:
                amount = line.get('debit', 0) - line.get('credit', 0)
                current_expense += amount
                if entry_month in monthly_data:
                    monthly_data[entry_month]['expense'] += amount
                if acc_code not in expense_by_account:
                    acc_info = next((a for a in expense_accounts if a['account_code'] == acc_code), {})
                    expense_by_account[acc_code] = {"account_code": acc_code, "account_name": acc_info.get('account_name_ar', acc_code), "amount": 0}
                expense_by_account[acc_code]['amount'] += amount
    
    # Bank balance
    bank_accounts = await target_db.chart_of_accounts.find({"$or": [{"account_name_ar": {"$regex": "بنك|صندوق|نقد", "$options": "i"}}, {"account_code": {"$regex": "^111"}}]}, {"account_code": 1, "_id": 0}).to_list(50)
    bank_codes = [acc['account_code'] for acc in bank_accounts]
    all_entries = await target_db.journal_entries.find({}).to_list(10000)
    bank_balance = 0
    for entry in all_entries:
        for line in entry.get('lines', []):
            if line['account_code'] in bank_codes:
                bank_balance += line.get('debit', 0) - line.get('credit', 0)
    
    chart_data = [{"month": month_names.get(m, str(m)), "revenue": round(monthly_data[m]['revenue'], 2), "expense": round(monthly_data[m]['expense'], 2), "net": round(monthly_data[m]['revenue'] - monthly_data[m]['expense'], 2)} for m in range(start_month, end_month + 1)]
    
    report_data = {
        "period": {"year": year, "quarter": quarter, "quarter_name": f"Q{quarter}", "start_date": start_date, "end_date": end_date},
        "summary": {
            "carried_forward": {"revenue": round(carried_forward_revenue, 2), "expense": round(carried_forward_expense, 2), "net": round(carried_forward_revenue - carried_forward_expense, 2)},
            "current_period": {"revenue": round(current_revenue, 2), "expense": round(current_expense, 2), "net": round(current_revenue - current_expense, 2)},
            "closing_balance": {"revenue": round(carried_forward_revenue + current_revenue, 2), "expense": round(carried_forward_expense + current_expense, 2), "net": round((carried_forward_revenue + current_revenue) - (carried_forward_expense + current_expense), 2)},
            "bank_balance": round(bank_balance, 2)
        },
        "chart_data": chart_data,
        "top_revenue_accounts": sorted(revenue_by_account.values(), key=lambda x: x['amount'], reverse=True)[:10],
        "top_expense_accounts": sorted(expense_by_account.values(), key=lambda x: x['amount'], reverse=True)[:10],
        "totals": {
            "total_revenue": round(current_revenue, 2),
            "total_expense": round(current_expense, 2),
            "net_profit": round(current_revenue - current_expense, 2),
            "profit_margin": round((current_revenue - current_expense) / current_revenue * 100, 2) if current_revenue > 0 else 0
        }
    }
    
    # Get comparison data
    comparison_data = None
    try:
        quarters_to_fetch = []
        for i in range(4):
            q = quarter - i
            y = year
            while q <= 0:
                q += 4
                y -= 1
            quarters_to_fetch.append({"year": y, "quarter": q})
        quarters_to_fetch.reverse()
        
        comparison = []
        for period_item in quarters_to_fetch:
            y, q = period_item['year'], period_item['quarter']
            historical = await target_db.historical_year_data.find_one({"year": y})
            qm = {1: (1, 3), 2: (4, 6), 3: (7, 9), 4: (10, 12)}
            sm, em = qm[q]
            sd = f"{y}-{sm:02d}-01"
            ed = f"{y}-{em:02d}-31"
            entries = await target_db.journal_entries.find({"entry_date": {"$gte": sd, "$lte": ed}}).to_list(5000)
            rev, exp = 0, 0
            for e in entries:
                for ln in e.get('lines', []):
                    if ln['account_code'] in revenue_codes:
                        rev += ln.get('credit', 0) - ln.get('debit', 0)
                    elif ln['account_code'] in expense_codes:
                        exp += ln.get('debit', 0) - ln.get('credit', 0)
            if rev == 0 and exp == 0 and historical:
                qf = {1: ('q1_revenue', 'q1_expense'), 2: ('q2_revenue', 'q2_expense'), 3: ('q3_revenue', 'q3_expense'), 4: ('q4_revenue', 'q4_expense')}
                rf, ef = qf[q]
                rev = historical.get(rf, 0)
                exp = historical.get(ef, 0)
            comparison.append({"year": y, "quarter": q, "label": f"Q{q} {y}", "revenue": round(rev, 2), "expense": round(exp, 2), "net_profit": round(rev - exp, 2), "is_current": (y == year and q == quarter), "has_data": rev > 0 or exp > 0})
        comparison_data = {"current_period": {"year": year, "quarter": quarter}, "comparison": comparison}
    except:
        pass
    
    # Get company settings
    company_settings = await target_db.company_settings.find_one({}, {"_id": 0})
    
    # Generate PDF
    pdf_buffer = generate_quarterly_report_pdf(report_data, comparison_data, company_settings)
    
    filename = f"quarterly_report_Q{quarter}_{year}.pdf"
    return StreamingResponse(
        pdf_buffer,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@api_router.get("/reports/quarterly/export/pptx")
async def export_quarterly_report_pptx(year: int, quarter: int, x_tenant_id: str = Header(None)):
    """Export quarterly report as PowerPoint"""
    target_db = await get_current_tenant_db(x_tenant_id) if x_tenant_id else db
    
    # Get report data (same logic as PDF export)
    quarter_months = {1: (1, 3), 2: (4, 6), 3: (7, 9), 4: (10, 12)}
    start_month, end_month = quarter_months.get(quarter, (1, 3))
    start_date = f"{year}-{start_month:02d}-01"
    end_date = f"{year}-{end_month:02d}-31"
    
    revenue_accounts = await target_db.chart_of_accounts.find({"account_type": "revenue"}, {"account_code": 1, "account_name_ar": 1, "_id": 0}).to_list(500)
    expense_accounts = await target_db.chart_of_accounts.find({"account_type": "expense"}, {"account_code": 1, "account_name_ar": 1, "_id": 0}).to_list(500)
    revenue_codes = [acc['account_code'] for acc in revenue_accounts]
    expense_codes = [acc['account_code'] for acc in expense_accounts]
    
    quarter_entries = await target_db.journal_entries.find({"entry_date": {"$gte": start_date, "$lte": end_date}}).to_list(5000)
    prev_entries = await target_db.journal_entries.find({"entry_date": {"$lt": start_date}}).to_list(5000)
    
    carried_forward_revenue, carried_forward_expense = 0, 0
    for entry in prev_entries:
        for line in entry.get('lines', []):
            if line['account_code'] in revenue_codes:
                carried_forward_revenue += line.get('credit', 0) - line.get('debit', 0)
            elif line['account_code'] in expense_codes:
                carried_forward_expense += line.get('debit', 0) - line.get('credit', 0)
    
    current_revenue, current_expense = 0, 0
    revenue_by_account, expense_by_account = {}, {}
    monthly_data = {m: {"month": m, "revenue": 0, "expense": 0} for m in range(start_month, end_month + 1)}
    month_names = {1: "يناير", 2: "فبراير", 3: "مارس", 4: "أبريل", 5: "مايو", 6: "يونيو", 7: "يوليو", 8: "أغسطس", 9: "سبتمبر", 10: "أكتوبر", 11: "نوفمبر", 12: "ديسمبر"}
    
    for entry in quarter_entries:
        entry_date = str(entry.get('entry_date', ''))[:10]
        try:
            entry_month = int(entry_date[5:7])
        except:
            entry_month = start_month
        
        for line in entry.get('lines', []):
            acc_code = line['account_code']
            if acc_code in revenue_codes:
                amount = line.get('credit', 0) - line.get('debit', 0)
                current_revenue += amount
                if entry_month in monthly_data:
                    monthly_data[entry_month]['revenue'] += amount
                if acc_code not in revenue_by_account:
                    acc_info = next((a for a in revenue_accounts if a['account_code'] == acc_code), {})
                    revenue_by_account[acc_code] = {"account_code": acc_code, "account_name": acc_info.get('account_name_ar', acc_code), "amount": 0}
                revenue_by_account[acc_code]['amount'] += amount
            elif acc_code in expense_codes:
                amount = line.get('debit', 0) - line.get('credit', 0)
                current_expense += amount
                if entry_month in monthly_data:
                    monthly_data[entry_month]['expense'] += amount
                if acc_code not in expense_by_account:
                    acc_info = next((a for a in expense_accounts if a['account_code'] == acc_code), {})
                    expense_by_account[acc_code] = {"account_code": acc_code, "account_name": acc_info.get('account_name_ar', acc_code), "amount": 0}
                expense_by_account[acc_code]['amount'] += amount
    
    bank_accounts = await target_db.chart_of_accounts.find({"$or": [{"account_name_ar": {"$regex": "بنك|صندوق|نقد", "$options": "i"}}, {"account_code": {"$regex": "^111"}}]}, {"account_code": 1, "_id": 0}).to_list(50)
    bank_codes = [acc['account_code'] for acc in bank_accounts]
    all_entries = await target_db.journal_entries.find({}).to_list(10000)
    bank_balance = 0
    for entry in all_entries:
        for line in entry.get('lines', []):
            if line['account_code'] in bank_codes:
                bank_balance += line.get('debit', 0) - line.get('credit', 0)
    
    chart_data = [{"month": month_names.get(m, str(m)), "revenue": round(monthly_data[m]['revenue'], 2), "expense": round(monthly_data[m]['expense'], 2), "net": round(monthly_data[m]['revenue'] - monthly_data[m]['expense'], 2)} for m in range(start_month, end_month + 1)]
    
    report_data = {
        "period": {"year": year, "quarter": quarter, "quarter_name": f"Q{quarter}", "start_date": start_date, "end_date": end_date},
        "summary": {
            "carried_forward": {"revenue": round(carried_forward_revenue, 2), "expense": round(carried_forward_expense, 2), "net": round(carried_forward_revenue - carried_forward_expense, 2)},
            "current_period": {"revenue": round(current_revenue, 2), "expense": round(current_expense, 2), "net": round(current_revenue - current_expense, 2)},
            "closing_balance": {"revenue": round(carried_forward_revenue + current_revenue, 2), "expense": round(carried_forward_expense + current_expense, 2), "net": round((carried_forward_revenue + current_revenue) - (carried_forward_expense + current_expense), 2)},
            "bank_balance": round(bank_balance, 2)
        },
        "chart_data": chart_data,
        "top_revenue_accounts": sorted(revenue_by_account.values(), key=lambda x: x['amount'], reverse=True)[:10],
        "top_expense_accounts": sorted(expense_by_account.values(), key=lambda x: x['amount'], reverse=True)[:10],
        "totals": {
            "total_revenue": round(current_revenue, 2),
            "total_expense": round(current_expense, 2),
            "net_profit": round(current_revenue - current_expense, 2),
            "profit_margin": round((current_revenue - current_expense) / current_revenue * 100, 2) if current_revenue > 0 else 0
        }
    }
    
    # Get comparison data
    comparison_data = None
    try:
        quarters_to_fetch = []
        for i in range(4):
            q = quarter - i
            y = year
            while q <= 0:
                q += 4
                y -= 1
            quarters_to_fetch.append({"year": y, "quarter": q})
        quarters_to_fetch.reverse()
        
        comparison = []
        for period_item in quarters_to_fetch:
            y, q = period_item['year'], period_item['quarter']
            historical = await target_db.historical_year_data.find_one({"year": y})
            qm = {1: (1, 3), 2: (4, 6), 3: (7, 9), 4: (10, 12)}
            sm, em = qm[q]
            sd = f"{y}-{sm:02d}-01"
            ed = f"{y}-{em:02d}-31"
            entries = await target_db.journal_entries.find({"entry_date": {"$gte": sd, "$lte": ed}}).to_list(5000)
            rev, exp = 0, 0
            for e in entries:
                for ln in e.get('lines', []):
                    if ln['account_code'] in revenue_codes:
                        rev += ln.get('credit', 0) - ln.get('debit', 0)
                    elif ln['account_code'] in expense_codes:
                        exp += ln.get('debit', 0) - ln.get('credit', 0)
            if rev == 0 and exp == 0 and historical:
                qf = {1: ('q1_revenue', 'q1_expense'), 2: ('q2_revenue', 'q2_expense'), 3: ('q3_revenue', 'q3_expense'), 4: ('q4_revenue', 'q4_expense')}
                rf, ef = qf[q]
                rev = historical.get(rf, 0)
                exp = historical.get(ef, 0)
            comparison.append({"year": y, "quarter": q, "label": f"Q{q} {y}", "revenue": round(rev, 2), "expense": round(exp, 2), "net_profit": round(rev - exp, 2), "is_current": (y == year and q == quarter), "has_data": rev > 0 or exp > 0})
        comparison_data = {"current_period": {"year": year, "quarter": quarter}, "comparison": comparison}
    except:
        pass
    
    company_settings = await target_db.company_settings.find_one({}, {"_id": 0})
    
    # Generate PPTX
    pptx_buffer = generate_quarterly_report_pptx(report_data, comparison_data, company_settings)
    
    filename = f"quarterly_report_Q{quarter}_{year}.pptx"
    return StreamingResponse(
        pptx_buffer,
        media_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

# Include router
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()