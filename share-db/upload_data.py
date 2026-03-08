import pandas as pd
from sqlalchemy import create_engine, text
import os

# --- 1. Configuration ---
# เชื่อมต่อไปยัง Port 5433 ที่เราเปิดไว้ใน ShareDB
BASE_URL = "postgresql://postgres:postgres@localhost:5433/"
DB_NAME = "greenery"
DATA_DIR = "data"

# --- 2. สร้าง Database 'greenery' (ถ้ายังไม่มี) ---
# เราต้องเชื่อมต่อไปที่ database 'postgres' ก่อนเพื่อสั่งสร้าง database ใหม่
temp_engine = create_engine(f"{BASE_URL}postgres")
with temp_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
    exists = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname='{DB_NAME}'")).fetchone()
    if not exists:
        conn.execute(text(f"CREATE DATABASE {DB_NAME}"))
        print(f"--- Created database: {DB_NAME} ---")
    else:
        print(f"--- Database '{DB_NAME}' already exists ---")

# --- 3. เชื่อมต่อไปที่ DB 'greenery' จริงๆ ---
engine = create_engine(f"{BASE_URL}{DB_NAME}")

# --- 4. นิยาม Schema (แปลงจาก Spark StructType ที่คุณส่งมาเป๊ะๆ) ---
# การใส่ dtype เป็น str จะป้องกันไม่ให้ zipcode หรือ id หลุดเป็น int
schemas = {
    "addresses": {"address_id": str, "address": str, "zipcode": str, "state": str, "country": str},
    "order_items": {"order_id": str, "product_id": str, "quantity": int},
    "products": {"product_id": str, "name": str, "price": float, "inventory": int},
    "promos": {"promo_id": str, "discount": int, "status": str},
    "events": {"event_id": str, "session_id": str, "page_url": str, "event_type": str, "user": str, "order": str, "product": str},
    "orders": {"order_id": str, "order_cost": float, "shipping_cost": float, "order_total": float, "tracking_id": str, "shipping_service": str, "status": str, "user": str, "promo": str, "address": str},
    "users": {"user_id": str, "first_name": str, "last_name": str, "email": str, "phone_number": str, "address_id": str}
}

# รายชื่อคอลัมน์ที่เป็น Timestamp (ที่เราคุยกันว่าต้องแยกมาจัดการ)
date_columns = ["created_at", "updated_at", "delivered_at", "estimated_delivery_at"]

# --- 5. เริ่มลูปโหลดไฟล์จากโฟลเดอร์ data/ ---
for table, dtype_map in schemas.items():
    file_path = os.path.join(DATA_DIR, f"{table}.csv")
    
    if os.path.exists(file_path):
        print(f"Processing table: {table}...")
        
        # อ่านไฟล์ CSV พร้อมล็อค Type ตาม Schema
        df = pd.read_csv(file_path, dtype=dtype_map)
        
        # แปลงคอลัมน์วันที่ (ถ้ามีในตารางนั้น) ให้เป็น DateTime Object
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        # ยัดข้อมูลลง Postgres (if_exists='replace' คือการ overwrite ข้อมูลเดิม)
        df.to_sql(table, engine, if_exists='replace', index=False)
        print(f"Successfully uploaded {table} ({len(df)} rows)")
    else:
        print(f"!!! Warning: File {file_path} not found. Skipping...")

print("\n--- [FINISHED] All data is now in dataengineer-bootcamp-db (greenery) ---")