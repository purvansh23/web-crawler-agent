import asyncio
import sys
import pandas as pd
from celery_app import celery_app
from core.database import DBManager
from core.crawler import Crawler
from core.matcher import Matcher
from core.ai_validator import AIValidator

# Initialize these outside the task to reuse them per worker process
# DBManager now handles its own SQLAlchemy pool per worker process, making it perfectly concurrent.
db = DBManager()
matcher = Matcher()
ai = AIValidator()

async def async_process(comp_id: str):
    """The async crawler/AI payload that must be run by the synchronous celery wrapper."""
    company = db.get_company(comp_id)
    if not company:
        return False
        
    name = company['company_name']
    url = company['website']
    
    if not url or pd.isna(url) or url == 'nan':
        db.update_result(comp_id, "done", False, "")
        return False
        
    print(f"[{comp_id}] Distributed Processing: {name} ({url})")
    crawler = Crawler(max_pages=10)
    pages_to_crawl = await crawler.get_priority_pages(url)
    
    matched_urls = []
    is_match = False
    crawl_failed = False  # Track if ALL pages failed to load
    pages_attempted = 0
    pages_failed = 0
    
    for page_url in pages_to_crawl:
        print(f"  [{comp_id}] -> Crawling: {page_url}")
        pages_attempted += 1
        text = await crawler.get_page_text(page_url)
        
        if not text or len(text.strip()) < 50:
            pages_failed += 1
            continue  # Skip this page, try the next one
        
        # 1. Deterministic Match
        if matcher.has_primary_match(text):
            print(f"    [{comp_id}] [REGEX HIT] Found primary keywords on {page_url}")
            
            # 2. AI Contextual Validation
            snippet = matcher.extract_snippet(text)
            if ai.validate(name, page_url, snippet):
                print(f"    [{comp_id}] [AI APPROVED] Verified valid service context for {name}")
                matched_urls.append(page_url)
                is_match = True
                break
            else:
                print(f"    [{comp_id}] [AI REJECTED] False positive context for {name}")
    
    # If every single page we tried returned empty/failed, flag this company for review
    if pages_attempted > 0 and pages_failed == pages_attempted:
        crawl_failed = True
        print(f"  [{comp_id}] [CRAWL FAILED] All {pages_attempted} pages returned empty - website may be fully blocked. Flagged for review.")

    final_urls = ",".join(matched_urls)
    db.update_result(comp_id, "done", is_match, final_urls, crawl_failed=crawl_failed)
    return is_match

@celery_app.task(name='tasks.process_company')
def process_company(comp_id: str):
    """
    Celery task entry point.
    Since Celery workers run synchronously by default, we use asyncio.run.
    On Windows, Celery disrupts the default async loop, causing Playwright subprocesses to crash.
    We enforce WindowsProactorEventLoopPolicy to fix the NotImplementedError.
    """
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
    return asyncio.run(async_process(comp_id))
