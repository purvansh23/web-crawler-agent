import os
from sqlalchemy import create_engine, Column, String, Boolean, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import func
import pandas as pd
from typing import List, Tuple
from dotenv import load_dotenv

load_dotenv()

# Get DB config from env
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "agentpass")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "cross_dock_db")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# SQLAlchemy Base configuration
Base = declarative_base()

class CompanyModel(Base):
    __tablename__ = 'companies'
    id = Column(String, primary_key=True)
    company_name = Column(String)
    city = Column(String)
    state = Column(String)
    zip = Column(String)
    website = Column(String)
    status = Column(String, default='pending')
    result = Column(Boolean)
    matched_urls = Column(String)
    crawl_failed = Column(Boolean, default=False)  # True if ALL pages were unreadable
    processed_at = Column(DateTime, server_default=func.now())

class DBManager:
    def __init__(self):
        # Create robust connection pool for Celery distributed workers
        self.engine = create_engine(
            DATABASE_URL,
            pool_size=20,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800
        )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self._init_db()

    def _init_db(self):
        # Create tables automatically
        Base.metadata.create_all(bind=self.engine)

    def load_from_excel(self, excel_path: str):
        # Using chunksize to prevent huge initial RAM spikes from Pandas
        print(f"Loading {excel_path} into RAM...")
        df = pd.read_excel(excel_path)
        if 'Company_ID' not in df.columns:
            df['Company_ID'] = [f'comp_{i}' for i in range(len(df))]
        
        print("Formatting data for bulk insertion...")
        # Prepare dictionaries for bulk insert
        records = []
        for _, row in df.iterrows():
            records.append({
                'id': str(row.get('Company_ID')),
                'company_name': str(row.get('Company_Name', '')),
                'city': str(row.get('City', '')),
                'state': str(row.get('State', '')),
                'zip': str(row.get('Zip', '')),
                'website': str(row.get('Website', '')),
                'status': 'pending'
            })
            
        print("Pushing bulk inserts to PostgreSQL...")
        from sqlalchemy.dialects.postgresql import insert
        
        with self.SessionLocal() as session:
            # We insert the records in chunks of 10,000 to keep the network query size manageable
            chunk_size = 10000
            for i in range(0, len(records), chunk_size):
                chunk = records[i:i + chunk_size]
                # PostgreSQL specific "INSERT OR IGNORE" bulk formulation
                stmt = insert(CompanyModel).values(chunk)
                stmt = stmt.on_conflict_do_nothing(index_elements=['id'])
                session.execute(stmt)
                
            session.commit()
        print("Bulk Ingestion complete!")

    def get_company(self, comp_id: str) -> dict:
        """Fetch a specific company by ID. Used by Celery Workers."""
        with self.SessionLocal() as session:
            comp = session.query(CompanyModel).filter(CompanyModel.id == str(comp_id)).first()
            if comp:
                return {
                    'id': comp.id,
                    'company_name': comp.company_name,
                    'website': comp.website
                }
            return None

    def get_pending_batch(self, limit: int = 50) -> List[dict]:
        """Gets a batch of pending companies. Now primarily used to seed the Celery Queue from main.py."""
        with self.SessionLocal() as session:
            # We select and lock for update to prevent concurrent race conditions on queue generation
            companies = session.query(CompanyModel).filter(CompanyModel.status == 'pending').limit(limit).with_for_update().all()
            
            result = []
            for comp in companies:
                comp.status = 'queued'  # Mark as queued in Redis
                result.append({
                    'id': comp.id,
                    'company_name': comp.company_name,
                    'website': comp.website
                })
            session.commit()
            return result

    def reset_stuck_tasks(self) -> int:
        """Resets all 'queued' tasks back to 'pending'. Useful if workers crash mid-execution."""
        with self.SessionLocal() as session:
            count = session.query(CompanyModel).filter(CompanyModel.status == 'queued').update({"status": "pending"})
            session.commit()
            return count

    def update_result(self, comp_id: str, status: str, result: bool = False, matched_urls: str = "", crawl_failed: bool = False):
        with self.SessionLocal() as session:
            comp = session.query(CompanyModel).filter(CompanyModel.id == str(comp_id)).first()
            if comp:
                comp.status = status
                comp.result = result
                comp.matched_urls = matched_urls
                comp.crawl_failed = crawl_failed
            session.commit()

    def get_stats(self) -> Tuple[int, int]:
        with self.SessionLocal() as session:
            total_done = session.query(CompanyModel).filter(CompanyModel.status == 'done').count()
            total_matches = session.query(CompanyModel).filter(CompanyModel.result == True).count()
            return total_done, total_matches

    def get_queued_count(self) -> int:
        with self.SessionLocal() as session:
            return session.query(CompanyModel).filter(CompanyModel.status == 'queued').count()

    def export_success_to_excel(self, output_path: str):
        # Main export: all confirmed True matches
        query = "SELECT id as Company_ID, company_name as Company_Name, city as City, state as State, zip as Zip, website as Website, matched_urls as Page_Path FROM companies WHERE result = true"
        df = pd.read_sql_query(query, self.engine)
        df.to_excel(output_path, index=False)
        
        # Also export crawl-failed companies as a separate review sheet
        failed_path = output_path.replace('.xlsx', '_FAILED_REVIEW.xlsx')
        failed_query = "SELECT id as Company_ID, company_name as Company_Name, city as City, state as State, zip as Zip, website as Website FROM companies WHERE crawl_failed = true"
        df_failed = pd.read_sql_query(failed_query, self.engine)
        if len(df_failed) > 0:
            df_failed.to_excel(failed_path, index=False)
            print(f"⚠️  {len(df_failed)} companies had crawl failures and could not be read. Saved to: {failed_path}")
            print(f"   These websites were fully blocked. You may want to manually review these.")

    def close(self):
        self.engine.dispose()
