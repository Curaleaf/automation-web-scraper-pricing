"""
Dispensary scraping tools - Pydantic AI tools converted from working notebook patterns.

CRITICAL: All regex patterns, rate limiting, and Florida store detection logic 
preserved exactly from proven working notebook implementation.
"""

import asyncio
import re
import random
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urljoin
from playwright.async_api import TimeoutError as PWTimeout

from pydantic_ai import RunContext
from .models import ProductData
from .dependencies import ScrapingDependencies

# CRITICAL: Preserve exact regex patterns from working notebook
BASE = "https://www.trulieve.com"
DISPENSARIES_URL = f"{BASE}/dispensaries"

PRICE_RE = re.compile(r"\$\s*([0-9]+(?:\.[0-9]{2})?)")
SIZE_RE = re.compile(r"\b(0\.5g|1g|2g|3\.5g|7g|10g|14g|28g)\b", re.I)
THC_SINGLE_RE = re.compile(r"\bTHC\b[^0-9]*([0-9]+(?:\.[0-9]+)?)\s*%", re.I)
THC_RANGE_RE = re.compile(r"\bTHC\b[^0-9]*([0-9]+(?:\.[0-9]+)?)\s*%[^0-9]+([0-9]+(?:\.[0-9]+)?)\s*%", re.I)

# CRITICAL: Size standardization mapping is essential for price_per_g calculations
SIZE_MAP = {"0.5g": 0.5, "1g": 1.0, "2g": 2.0, "3.5g": 3.5, "7g": 7.0, "10g": 10.0, "14g": 14.0, "28g": 28.0}


def grams_from_size(s: Optional[str]) -> Optional[float]:
    """Convert size string to grams using proven size mapping."""
    return SIZE_MAP.get((s or "").lower())


def looks_like_florida(href: Optional[str], text: Optional[str]) -> bool:
    """
    CRITICAL: Florida store detection regex patterns are proven and should be preserved.
    
    Args:
        href: Store URL href
        text: Store text content
        
    Returns:
        bool: True if store appears to be in Florida
    """
    t = (text or "").upper()
    h = (href or "").lower()
    return (
        (", FL" in t) or 
        t.endswith(" FL") or 
        " FL " in t or 
        any(v in h for v in ("/florida", "-fl-")) or 
        h.endswith(("/fl", "-fl"))
    )


def product_slug(href: Optional[str]) -> str:
    """Extract product slug from href for deduplication key."""
    try:
        return href.split("/product/", 1)[1].split("?", 1)[0].split("#", 1)[0].strip("/")
    except:
        return href or ""


async def extract_fl_store_links(page) -> List[Tuple[str, str]]:
    """
    CRITICAL: PRESERVE extract_fl_store_links() async function exactly.
    
    Extract Florida store links from dispensaries page.
    
    Args:
        page: Playwright page object
        
    Returns:
        List of (store_name, store_url) tuples for Florida stores
    """
    await page.goto(DISPENSARIES_URL, wait_until="networkidle")
    anchors = await page.locator("a[href^='/dispensaries/']").all()
    out, seen = [], set()
    
    for a in anchors:
        href = await a.get_attribute("href")
        txt = (await a.text_content() or "").strip()
        if href and "/dispensaries/" in href and href not in seen and looks_like_florida(href, txt):
            seen.add(href)
            out.append((" ".join(txt.split()), urljoin(BASE, href)))
    
    # Deduplicate by name
    uniq, names = [], set()
    for name, url in out:
        if name not in names:
            names.add(name)
            uniq.append((name, url))
    
    return uniq


async def load_all(page) -> None:
    """
    CRITICAL: PRESERVE load_all() pagination logic exactly.
    
    Load all products by clicking "Load More" buttons until all content loaded.
    
    Args:
        page: Playwright page object
    """
    while True:
        await page.mouse.wheel(0, 40000)
        # CRITICAL: Rate limiting is essential - random delays 700-1500ms between requests
        await page.wait_for_timeout(random.randint(800, 1400))
        
        btn = page.get_by_role("button", name=re.compile(r"Load More", re.I))
        if await btn.count() and await btn.first().is_visible():
            try:
                await btn.first().click()
                await page.wait_for_timeout(random.randint(1000, 1600))
                continue
            except:
                pass
        break


async def extract_price_from_card(card) -> Optional[float]:
    """
    CRITICAL: PRESERVE extract_price_from_card() function exactly.
    
    Extract price from product card using proven locator patterns.
    
    Args:
        card: Playwright locator for product card
        
    Returns:
        Minimum price found or None
    """
    c = card.locator(".price, [class*='price'], :text('$'):not(:text('Add to Wishlist'))")
    try:
        n = min(await c.count(), 4)
        texts = []
        for i in range(n):
            t = await c.nth(i).text_content()
            if t and "$" in t:
                texts.append(t)
        blob = " ".join(texts) if texts else (await card.inner_text())
    except:
        blob = (await card.inner_text())
    
    vals = [float(m.group(1)) for m in PRICE_RE.finditer(blob or "")]
    return min(vals) if vals else None


async def extract_price_from_pdp(ctx, url: str) -> Optional[float]:
    """
    CRITICAL: PRESERVE extract_price_from_pdp() function exactly.
    
    Extract price from Product Detail Page as fallback.
    
    Args:
        ctx: Browser context
        url: Product URL
        
    Returns:
        Minimum price found or None
    """
    try:
        p = await ctx.new_page()
        await p.goto(url, wait_until="domcontentloaded", timeout=20000)
        body = await p.locator("body").inner_text()
        await p.close()
        vals = [float(m.group(1)) for m in PRICE_RE.finditer(body or "")]
        return min(vals) if vals else None
    except (PWTimeout, Exception):
        return None


async def extract_brand_from_card(card) -> Optional[str]:
    """
    CRITICAL: PRESERVE extract_brand_from_card() function exactly.
    
    Extract brand from product card using proven locator patterns.
    
    Args:
        card: Playwright locator for product card
        
    Returns:
        Brand name or None
    """
    try:
        bel = card.locator(".ProductCard_brand, .brand, .c-product-card__brand, [class*='Brand'], [data-testid*='brand']")
        if await bel.count():
            txt = (await bel.first.text_content() or "").strip()
            if txt:
                return txt
    except:
        pass
    return None


async def extract_brand_from_pdp(ctx, url: str) -> Optional[str]:
    """
    CRITICAL: PRESERVE extract_brand_from_pdp() function exactly.
    
    Extract brand from Product Detail Page as fallback.
    
    Args:
        ctx: Browser context
        url: Product URL
        
    Returns:
        Brand name or None
    """
    try:
        p = await ctx.new_page()
        await p.goto(url, wait_until="domcontentloaded", timeout=20000)
        
        # breadcrumb / meta
        crumbs = p.locator("nav a, .breadcrumb a, [class*='breadcrumb'] a")
        if await crumbs.count():
            vals = []
            for i in range(min(5, await crumbs.count())):
                t = (await crumbs.nth(i).text_content() or "").strip()
                if t:
                    vals.append(t)
            for v in vals:
                if v.lower() not in ("home", "flower", "pre-rolls", "minis", "ground & shake", "products", "shop"):
                    if 1 <= len(v) <= 40:
                        await p.close()
                        return v
        
        # labeled line
        label = p.locator("text=/Brand\\s*:/i")
        if await label.count():
            line = await label.first.text_content()
            if line and ":" in line:
                b = line.split(":", 1)[1].strip()
                if b:
                    await p.close()
                    return b
        
        meta = p.locator("[data-brand], [itemprop='brand'], [class*='brand']")
        if await meta.count():
            t = (await meta.first.text_content() or "").strip()
            if t:
                await p.close()
                return t
        
        body = await p.locator("body").inner_text()
        m = re.search(r"Brand\s*[:\-]\s*([^\n\r]+)", body, flags=re.I)
        await p.close()
        return m.group(1).strip() if m else None
    except (PWTimeout, Exception):
        return None


async def scrape_category(page, ctx, store_name: str, category_url: str, subcategory: str) -> List[Dict[str, Any]]:
    """
    CRITICAL: PRESERVE scrape_category() main logic exactly.
    
    Scrape all products for a specific category from a store.
    
    Args:
        page: Playwright page object
        ctx: Playwright browser context
        store_name: Name of the store
        category_url: URL for the category page
        subcategory: Product subcategory name
        
    Returns:
        List of product data dictionaries
    """
    await page.goto(category_url, wait_until="domcontentloaded")
    await load_all(page)
    
    name_links = await page.locator("a[href*='/product/']:not(:has(img))").all()
    rows, seen = [], set()
    
    for link in name_links:
        try:
            name = ((await link.text_content()) or "").strip()
            if not name:
                continue
                
            href = await link.get_attribute("href")
            url = urljoin(BASE, href) if href else None
            slug = product_slug(href or "")
            card = link.locator("xpath=ancestor::*[self::article or self::li or self::div][1]")
            card_text = (await card.inner_text()) if await card.count() else name

            size = (SIZE_RE.search(card_text).group(1).lower() if SIZE_RE.search(card_text) else None)
            grams = grams_from_size(size)

            price = await extract_price_from_card(card)
            brand = await extract_brand_from_card(card)
            
            # Fallback to PDP for missing data
            if (price is None or brand is None) and url:
                if price is None:
                    p = await extract_price_from_pdp(ctx, url)
                    if p is not None:
                        price = p
                if brand is None:
                    b = await extract_brand_from_pdp(ctx, url)
                    if b:
                        brand = b

            # Extract strain type
            strain_type = None
            st_el = card.locator("text=/\\b(Indica|Sativa|Hybrid)\\b/i")
            if await st_el.count():
                t = (await st_el.first.text_content()) or ""
                m = re.search(r"(Indica|Sativa|Hybrid)", t, re.I)
                if m:
                    strain_type = m.group(1).capitalize()
            if not strain_type:
                for kw in ("Indica", "Sativa", "Hybrid"):
                    if re.search(rf"\b{kw}\b", card_text, re.I):
                        strain_type = kw
                        break

            # Extract THC percentage
            thc_pct = None
            mr = THC_RANGE_RE.search(card_text)
            if mr:
                try:
                    thc_pct = float(mr.group(1))
                except:
                    pass
            if thc_pct is None:
                ms = THC_SINGLE_RE.search(card_text)
                if ms:
                    try:
                        thc_pct = float(ms.group(1))
                    except:
                        pass

            # CRITICAL: Deduplication key prevents duplicates across store visits
            key = (store_name, slug, size, subcategory)
            if key in seen:
                continue
            seen.add(key)

            rows.append({
                "state": "FL",
                "store": store_name,
                "subcategory": subcategory,
                "name": name,
                "brand": brand,
                "strain_type": strain_type,
                "thc_pct": thc_pct,
                "size_raw": size,
                "grams": grams,
                "price": price,
                "price_per_g": (round(price / grams, 2) if price and grams else None),
                "url": url
            })
        except:
            continue
    
    return rows


# Pydantic AI tool functions
async def scrape_category_tool(
    ctx: RunContext[ScrapingDependencies],
    category_url: str,
    subcategory: str,
    store_name: str
) -> List[ProductData]:
    """
    Pydantic AI tool to scrape a category for a specific store.
    
    Args:
        ctx: Runtime context with dependencies
        category_url: URL for the category page
        subcategory: Product subcategory name
        store_name: Name of the store
        
    Returns:
        List of ProductData objects
    """
    page = await ctx.deps.playwright_context.new_page()
    
    try:
        rows = await scrape_category(page, ctx.deps.playwright_context, store_name, category_url, subcategory)
        return [ProductData(**row) for row in rows]
    finally:
        await page.close()


async def extract_florida_stores_tool(ctx: RunContext[ScrapingDependencies]) -> List[Tuple[str, str]]:
    """
    Pydantic AI tool to extract Florida store links.
    
    Args:
        ctx: Runtime context with dependencies
        
    Returns:
        List of (store_name, store_url) tuples
    """
    page = await ctx.deps.playwright_context.new_page()
    
    try:
        return await extract_fl_store_links(page)
    finally:
        await page.close()


async def scrape_store_category_tool(
    ctx: RunContext[ScrapingDependencies],
    store_name: str,
    store_url: str,
    category_url: str,
    subcategory: str
) -> List[ProductData]:
    """
    Pydantic AI tool to scrape a specific category for a specific store.
    
    Args:
        ctx: Runtime context with dependencies  
        store_name: Name of the store
        store_url: URL of the store
        category_url: URL for the category page
        subcategory: Product subcategory name
        
    Returns:
        List of ProductData objects
    """
    page = await ctx.deps.playwright_context.new_page()
    
    try:
        # CRITICAL: Rate limiting is essential - random delays between requests
        await page.wait_for_timeout(random.randint(700, 1500))
        
        # Navigate to store and click "Shop At This Store"
        try:
            await page.goto(store_url, wait_until="domcontentloaded")
            btn = page.get_by_role("button", name=re.compile(r"Shop At This Store", re.I))
            if await btn.count():
                try:
                    await btn.first().click()
                    await page.wait_for_timeout(random.randint(900, 1400))
                except:
                    pass
        except:
            return []
        
        # Scrape the category
        rows = await scrape_category(page, ctx.deps.playwright_context, store_name, category_url, subcategory)
        return [ProductData(**row) for row in rows]
    
    finally:
        await page.close()