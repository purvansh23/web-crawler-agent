import asyncio
import httpx
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import trafilatura
from playwright.async_api import async_playwright

class Crawler:
    def __init__(self, max_pages: int = 10, timeout: int = 15):
        self.max_pages = max_pages
        self.timeout = timeout
        # Using a browser-like user agent to avoid basic blocks
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

    def _normalize_url(self, url: str) -> str:
        """Ensures the URL has a scheme."""
        if not url.startswith('http://') and not url.startswith('https://'):
            return f'https://{url}'
        return url

    def _get_base_domain(self, url: str) -> str:
        return urlparse(self._normalize_url(url)).netloc

    async def get_sitemap_urls(self, client: httpx.AsyncClient, base_url: str) -> list[str]:
        """Attempts to find and parse standard sitemap locations."""
        urls = []
        sitemap_paths = ['/sitemap.xml', '/sitemap_index.xml', '/sitemap/']
        for path in sitemap_paths:
            try:
                r = await client.get(urljoin(base_url, path), timeout=self.timeout)
                if r.status_code == 200:
                    soup = BeautifulSoup(r.content, 'xml')
                    urls = [loc.text for loc in soup.find_all('loc')]
                    if urls: # Found valid URLs
                        break
            except Exception:
                continue
        return urls

    async def crawl_internal_links(self, client: httpx.AsyncClient, base_url: str) -> list[str]:
        """BFS fallback to find internal links from the homepage."""
        try:
            r = await client.get(base_url, timeout=self.timeout, follow_redirects=True)
            if r.status_code != 200:
                return [base_url]
            soup = BeautifulSoup(r.content, 'html.parser')
            base_domain = self._get_base_domain(str(r.url))
            
            links = set()
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                full_url = urljoin(str(r.url), href)
                # Check if it's an internal link
                if self._get_base_domain(full_url) == base_domain:
                    links.add(full_url)
            
            # Combine homepage with found links
            all_links = list(links)
            if base_url not in all_links and str(r.url) not in all_links:
                all_links.append(str(r.url))
            return all_links
        except Exception:
            return [base_url]

    def _score_url(self, url: str) -> int:
        """Prioritizes service and logistics related pages."""
        url_lower = url.lower()
        if 'services' in url_lower: return 100
        if 'logistics' in url_lower or 'supply-chain' in url_lower: return 90
        if 'solutions' in url_lower: return 80
        if 'warehouse' in url_lower or 'distribution' in url_lower: return 70
        if 'about' in url_lower: return 50
        if 'blog' in url_lower: return 10
        # Check if it looks like just the base domain (home)
        parsed = urlparse(url)
        if not parsed.path or parsed.path == '/': return 60
        return 20 # Other pages

    async def get_priority_pages(self, url: str) -> list[str]:
        """Returns ordered list of URLs to crawl."""
        normalized_url = self._normalize_url(url)
        async with httpx.AsyncClient(headers=self.headers, verify=False) as client: # verify=False for poorly configured SSLs
            urls = await self.get_sitemap_urls(client, normalized_url)
            
            # Filter out sitemap indexes (.xml) and media files
            bad_exts = ('.xml', '.png', '.jpg', '.jpeg', '.gif', '.pdf', '.css', '.js', '.mp4', '.svg', '#')
            urls = [u for u in set(urls) if not u.lower().endswith(bad_exts) and not self._is_media_url(u)]

            if not urls:
                urls = await self.crawl_internal_links(client, normalized_url)
                urls = [u for u in set(urls) if not u.lower().endswith(bad_exts) and not self._is_media_url(u)]
        
        # Sort by priority
        urls.sort(key=self._score_url, reverse=True)
        # Return top N
        return urls[:self.max_pages]

    def _is_media_url(self, url: str) -> bool:
        """Helper to catch poorly formatted media URLs that don't trivially end with the extension."""
        url_lower = url.lower()
        if any(ext in url_lower for ext in ['.jpg', '.png', '.jpeg', '.pdf', '.gif', '/wp-content/uploads/']):
            return True
        return False

    async def _extract_with_playwright(self, url: str) -> str:
        """Fallback Headless Chrome extractor. Retries once with longer timeout before giving up."""
        for attempt in range(2):  # Try up to 2 times
            try:
                timeout_ms = 25000 if attempt == 0 else 45000  # Longer timeout on retry
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    context = await browser.new_context(ignore_https_errors=True, user_agent=self.headers['User-Agent'])
                    page = await context.new_page()
                    await page.goto(url, timeout=timeout_ms, wait_until='domcontentloaded')
                    await page.wait_for_timeout(2000)
                    text = await page.inner_text('body')
                    await browser.close()
                    if text and len(text.strip()) > 50:
                        return text
            except Exception as e:
                if attempt == 0:
                    print(f"    [PLAYWRIGHT RETRY] First attempt failed on {url}, retrying with longer timeout...")
                else:
                    print(f"    [PLAYWRIGHT FAILED] Could not render {url} after 2 attempts")
        return ""

    async def get_page_text(self, url: str) -> str:
        """Extracts visible text using trafilatura, falling back to Playwright if needed."""
        text = ""
        is_blocked = False
        try:
            async with httpx.AsyncClient(headers=self.headers, verify=False) as client:
                r = await client.get(url, timeout=self.timeout, follow_redirects=True)
                if r.status_code == 200:
                    text = trafilatura.extract(r.content)
                    if text and len(text) > 100:
                        return text
                elif r.status_code in [403, 401]:
                    is_blocked = True
        except Exception:
            pass

        if is_blocked:
            print(f"    [BOT BLOCK] 403 on {url}. Triggering Headless Chrome Fallback...")
        else:
            print(f"    [SPA DETECTED] Empty text on {url}. Triggering Headless Chrome Fallback...")
            
        return await self._extract_with_playwright(url)
