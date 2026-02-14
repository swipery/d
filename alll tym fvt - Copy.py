import os
import sys
import time
import asyncio
import logging
from typing import Optional, List
from urllib.parse import unquote

from playwright.async_api import async_playwright, Page, BrowserContext, TimeoutError as PlaywrightTimeoutError

# =========================
# CONFIG
# =========================
SESSION_ID = input("Session ID: ").strip()
DM_URL = input("Group Chat URL: ").strip()
MESSAGE = """lofi papa 𓆩🌌𓆪 ━━━✧༺✦༻✧━━━✧༺✦༻✧━━━  lofi papa 𓆩🩵𓆪 ⋆⛧⋆⛧⋆⛧⋆⛧⋆⛧⋆⛧⋆⛧⋆  lofi papa 𓆩🌉𓆪 ═══✶═══✶═══✶═══✶═══  lofi papa 𓆩🦉𓆪 ༒︎✧༒︎✧༒︎✧༒︎✧༒︎✧༒︎  lofi papa 𓆩🎖️𓆪 ░▒▓★▓▒░★░▒▓★▓▒░  lofi papa 𓆩🙂𓆪 ⛧༺♰༻⛧༺♰༻⛧༺♰༻  lofi papa 𓆩🌿𓆪 ──✦────✦────✦──  lofi papa 𓆩✨𓆪 ꧁✧꧂꧁✧꧂꧁✧꧂  lofi papa 𓆩🌼𓆪 ◈━◈━◈━◈━◈━◈  lofi papa 𓆩🦒𓆪 ☾✶☽☾✶☽☾✶☽  lofi papa 𓆩🧭𓆪 ༻✦༺༻✦༺༻✦༺  lofi papa 𓆩🏠𓆪 ✧༚༚✧༚༚✧༚༚✧  lofi papa 𓆩🤍𓆪 ⛧━━⛧━━⛧━━⛧  lofi papa 𓆩⭐𓆪 ✦━━✦━━✦━━✦━━✦  lofi papa 𓆩🔧𓆪 ༒︎━━༒︎━━༒︎━━༒︎  lofi papa 𓆩🪰𓆪 ◈✧◈✧◈✧◈✧◈  lofi papa 𓆩🌗𓆪 ⛧══⛧══⛧══⛧"""
THREADS = 5
MAX_MESSAGES = int(input("Max messages (0 = infinite): ") or 0)

# =========================
# ULTRA-FAST TUNING
# =========================
TABS_PER_WORKER = 4              # More tabs = more parallelism
TARGET_RATE = 100.0              # 100 actions/sec globally (5x faster)
BASE_DELAY = 0.005               # Minimal delay between actions
RECYCLE_AFTER = 200              # Delay recycling (less overhead)
RECYCLE_COOLDOWN = 0.5           # Fast recycle
ERROR_BACKOFF = 1.0              # Shorter backoff on error
PAGE_RELOAD_BACKOFF = 2.0
VERIFY_SETTLE_MS = 50            # Faster DOM settle (reduced from 150)
PAGE_TIMEOUT = 30000             # Shorter timeout
HEALTH_CHECK_INTERVAL = 10
NAV_TIMEOUT = 20000

# Per-action delay accounting for parallelism
TOTAL_CONCURRENT = THREADS * TABS_PER_WORKER
BASE_DELAY = max(0.002, 1.0 / (TARGET_RATE / TOTAL_CONCURRENT))

# =========================
# LOGGING (minimal overhead)
# =========================
logging.basicConfig(
    level=logging.WARNING,  # Only warnings/errors to reduce I/O
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("fvt")

# =========================
# GLOBAL RATE LIMITER (aggressive)
# =========================
class RateLimiter:
    def __init__(self, rate: float):
        self.rate = rate
        self.tokens = rate
        self.lock = asyncio.Lock()
        self.last_update = time.time()
    
    async def acquire(self, tokens: int = 1):
        async with self.lock:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if self.tokens < tokens:
                wait_time = (tokens - self.tokens) / self.rate
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= tokens

RATE_LIMITER = RateLimiter(TARGET_RATE)

# =========================
# STATS
# =========================
class Stats:
    def __init__(self):
        self.sent = 0
        self.failed = 0
        self.recovered = 0
        self.recycled = 0
        self.lock = asyncio.Lock()

    async def inc_sent(self, n=1):
        async with self.lock:
            self.sent += n

    async def inc_failed(self, n=1):
        async with self.lock:
            self.failed += n

    async def inc_recovered(self, n=1):
        async with self.lock:
            self.recovered += n

    async def inc_recycled(self, n=1):
        async with self.lock:
            self.recycled += n

STATS = Stats()

# =========================
# HELPERS (optimized for speed)
# =========================
async def prepare_context(pw) -> BrowserContext:
    browser = await pw.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
            "--disable-extensions",
            "--disable-plugins",
            "--disable-images",  # Don't load images (faster)
            "--disable-media-session",
            "--disable-sync"
        ]
    )
    context = await browser.new_context(
        locale="en-US",
        viewport={"width": 1280, "height": 720},
        ignore_https_errors=True
    )
    await context.add_cookies([{
        "name": "sessionid",
        "value": unquote(SESSION_ID),
        "domain": ".instagram.com",
        "path": "/",
        "secure": True,
        "httpOnly": True,
        "sameSite": "None"
    }])
    return context

async def open_chat_page(context: BrowserContext, tab_id: int) -> Optional[Page]:
    page = None
    try:
        page = await context.new_page()
        
        # Disable images/CSS for speed
        await page.route("**/*.{png,jpg,jpeg,gif,svg,webp}", lambda route: route.abort())
        
        await page.goto(DM_URL, wait_until="networkidle", timeout=NAV_TIMEOUT)
        
        await page.wait_for_selector(
            'textarea[placeholder="Message..."], div[role="textbox"][contenteditable="true"]',
            timeout=PAGE_TIMEOUT
        )
        logger.info(f"Tab {tab_id}: Ready")
        return page
    except Exception as e:
        logger.warning(f"Tab {tab_id}: Open failed — {e}")
        if page:
            try:
                await page.close()
            except:
                pass
        return None

async def find_message_box(page: Page) -> Optional:
    try:
        box = page.locator(
            'textarea[placeholder="Message..."], div[role="textbox"][contenteditable="true"]'
        ).first
        await box.wait_for(state="visible", timeout=3000)
        return box
    except:
        return None

async def send_once(page: Page, tab_id: int) -> bool:
    try:
        box = await find_message_box(page)
        if not box:
            return False
        
        # Fast fill + send (no timeout, just try)
        try:
            await box.fill(MESSAGE, timeout=PAGE_TIMEOUT)
        except:
            return False
        
        try:
            await box.press("Enter", timeout=PAGE_TIMEOUT)
        except:
            return False
        
        # Minimal settle time
        await page.wait_for_timeout(VERIFY_SETTLE_MS)
        return True
    except:
        return False

# =========================
# TAB MANAGER (ultra-fast)
# =========================
class TabManager:
    def __init__(self, context: BrowserContext, worker_id: int, num_tabs: int):
        self.context = context
        self.worker_id = worker_id
        self.num_tabs = num_tabs
        self.pages: List[Optional[Page]] = [None] * num_tabs
        self.action_counts = [0] * num_tabs
    
    async def initialize(self) -> bool:
        # Start all tabs in parallel
        tasks = [
            open_chat_page(self.context, f"W{self.worker_id}T{i}")
            for i in range(self.num_tabs)
        ]
        pages = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, page in enumerate(pages):
            if isinstance(page, Exception) or page is None:
                logger.error(f"Worker {self.worker_id}: Tab {i} init failed")
                return False
            self.pages[i] = page
        return True
    
    async def close_all(self):
        tasks = [page.close() for page in self.pages if page]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.pages = [None] * self.num_tabs
    
    async def send_on_tab(self, tab_idx: int) -> bool:
        page = self.pages[tab_idx]
        if not page:
            return False
        
        try:
            ok = await send_once(page, f"W{self.worker_id}T{tab_idx}")
            if ok:
                self.action_counts[tab_idx] += 1
                return True
            else:
                # Quick recovery attempt
                try:
                    await page.reload(wait_until="networkidle", timeout=NAV_TIMEOUT)
                    await STATS.inc_recovered(1)
                    ok = await send_once(page, f"W{self.worker_id}T{tab_idx}")
                    if ok:
                        self.action_counts[tab_idx] += 1
                        return True
                except:
                    pass
            return False
        except:
            return False
    
    async def send_all_parallel(self) -> int:
        """Send on ALL tabs in parallel"""
        tasks = [self.send_on_tab(i) for i in range(self.num_tabs)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return sum(1 for r in results if r is True)
    
    async def recycle_if_needed(self):
        for i in range(self.num_tabs):
            if self.action_counts[i] >= RECYCLE_AFTER:
                try:
                    await self.pages[i].close()
                    await asyncio.sleep(RECYCLE_COOLDOWN)
                    new_page = await open_chat_page(self.context, f"W{self.worker_id}T{i}")
                    if new_page:
                        self.pages[i] = new_page
                        self.action_counts[i] = 0
                        await STATS.inc_recycled(1)
                except:
                    pass

# =========================
# WORKER (ultra-fast)
# =========================
async def worker(context: BrowserContext, worker_id: int):
    logger.info(f"Worker {worker_id}: Starting ({TABS_PER_WORKER} tabs)")
    
    tab_manager = TabManager(context, worker_id, TABS_PER_WORKER)
    
    if not await tab_manager.initialize():
        logger.error(f"Worker {worker_id}: Init failed")
        return
    
    sent_local = 0
    consecutive_fails = 0
    
    try:
        while True:
            if MAX_MESSAGES and sent_local >= MAX_MESSAGES:
                break
            
            try:
                # Send on all tabs in parallel
                success_count = await tab_manager.send_all_parallel()
                
                if success_count > 0:
                    sent_local += success_count
                    await STATS.inc_sent(success_count)
                    consecutive_fails = 0
                else:
                    await STATS.inc_failed(TABS_PER_WORKER)
                    consecutive_fails += 1
                
                # Quick backoff if too many failures
                if consecutive_fails > 5:
                    await asyncio.sleep(ERROR_BACKOFF)
                    consecutive_fails = 0
                
                # Recycle periodically
                await tab_manager.recycle_if_needed()
                
                # Apply global rate limit
                await RATE_LIMITER.acquire(TABS_PER_WORKER)
                
            except Exception as e:
                logger.error(f"Worker {worker_id}: {e}")
                await asyncio.sleep(ERROR_BACKOFF)
    
    except asyncio.CancelledError:
        pass
    finally:
        await tab_manager.close_all()

# =========================
# DASHBOARD (lightweight)
# =========================
async def dashboard(start_ts: float):
    while True:
        try:
            os.system("clear")
            elapsed = max(1, int(time.time() - start_ts))
            rate = STATS.sent / elapsed if elapsed > 0 else 0
            
            print("\n" + "="*50)
            print("  🚀 ULTRA-FAST DM SENDER 🚀")
            print("="*50)
            print(f"Workers              : {THREADS} × {TABS_PER_WORKER} tabs = {TOTAL_CONCURRENT} parallel")
            print(f"Target rate          : {TARGET_RATE:.0f} msg/s 🔥")
            print("-"*50)
            print(f"✓ Sent               : {STATS.sent}")
            print(f"✗ Failed             : {STATS.failed}")
            print(f"↻ Recovered          : {STATS.recovered}")
            print(f"⏱ Actual rate        : {rate:.2f} msg/s ⚡")
            print(f"⏰ Elapsed            : {elapsed}s")
            print("-"*50)
            print("CTRL+C to stop")
            print("="*50 + "\n")
            
            await asyncio.sleep(2)
        except:
            await asyncio.sleep(2)

# =========================
# MAIN
# =========================
async def main():
    start_ts = time.time()
    logger.info("="*50)
    logger.info("ULTRA-FAST MODE ACTIVATED")
    logger.info(f"Config: {THREADS}w × {TABS_PER_WORKER}t = {TOTAL_CONCURRENT} concurrent")
    logger.info(f"Target: {TARGET_RATE} msg/s")
    logger.info("="*50)
    
    try:
        async with async_playwright() as pw:
            context = await prepare_context(pw)
            
            worker_tasks = [
                asyncio.create_task(worker(context, i))
                for i in range(THREADS)
            ]
            
            dashboard_task = asyncio.create_task(dashboard(start_ts))
            all_tasks = worker_tasks + [dashboard_task]
            
            try:
                await asyncio.gather(*all_tasks)
            except KeyboardInterrupt:
                logger.info("\nShutdown...")
                for task in all_tasks:
                    task.cancel()
                await asyncio.gather(*all_tasks, return_exceptions=True)
    
    finally:
        elapsed = int(time.time() - start_ts)
        print("\n" + "="*50)
        print(f"FINAL: {STATS.sent} sent in {elapsed}s = {STATS.sent/elapsed:.2f} msg/s")
        print("="*50)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")