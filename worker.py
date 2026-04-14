import asyncio
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from core.database import DBManager
from core.crawler import Crawler
from core.matcher import Matcher
from core.ai_validator import AIValidator

class BatchWorker:
    def __init__(self, db_manager: DBManager, max_workers: int = 10):
        self.db = db_manager
        self.max_workers = max_workers
        self.matcher = Matcher()
        self.ai = AIValidator()

    async def _process_company_async(self, company: dict) -> bool:
        """Async core processing logic for a single company."""
        comp_id = company['id']
        name = company['company_name']
        url = company['website']
        
        if not url or pd.isna(url):
            self.db.update_result(comp_id, "done", False, "")
            return False

        print(f"[{comp_id}] Processing: {name} ({url})")
        
        crawler = Crawler(max_pages=10)
        pages_to_crawl = await crawler.get_priority_pages(url)
        
        matched_urls = []
        is_match = False
        
        for page_url in pages_to_crawl:
            print(f"  -> Crawling: {page_url}")
            text = await crawler.get_page_text(page_url)
            
            # 1. Deterministic Match (FREE & FAST)
            if self.matcher.has_primary_match(text):
                print(f"    [REGEX HIT] Found primary keywords on {page_url}")
                
                # 2. AI Contextual Validation
                snippet = self.matcher.extract_snippet(text)
                if self.ai.validate(name, page_url, snippet):
                    print(f"    [AI APPROVED] Verified valid service context for {name}")
                    matched_urls.append(page_url)
                    is_match = True
                    # If we only need to confirm IF they do it, we can break early
                    # to save time, or continue to find all matched URLs. The logic
                    # says Exclude if NO match, Include if ANY. Let's break early to save API calls.
                    break
                else:
                    print(f"    [AI REJECTED] False positive context for {name}")

        
        final_urls = ",".join(matched_urls)
        self.db.update_result(comp_id, "done", is_match, final_urls)
        return is_match

    def _sync_wrapper(self, company: dict):
        """Wrapper to run async code inside ThreadPool."""
        return asyncio.run(self._process_company_async(company))

    def run_batch(self, batch_size: int = 50):
        """Pulls a batch from the DB and processes concurrently via ThreadPool."""
        pending = self.db.get_pending_batch(limit=batch_size)
        if not pending:
            print("No pending companies found.")
            return False

        print(f"Starting batch of {len(pending)} companies using {self.max_workers} workers...")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # We map the sync wrapper over the pending items
            list(executor.map(self._sync_wrapper, pending))
            
        print(f"Batch completed.")
        return True
