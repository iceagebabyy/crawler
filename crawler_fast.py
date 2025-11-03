
import asyncio
import random
import re
import time
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from playwright_stealth import stealth_async

BASE = "https://www.otodom.pl"
LIST_URL = BASE + "/pl/wyniki/{mode}/{typ}/{lok}?page={page}&viewType=listing"

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


async def extract_offer_urls(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/pl/oferta/" in href:
            if href.startswith("/"):
                href = BASE + href
            urls.add(href.split("?")[0])
    return list(urls)

async def adaptive_sleep():
    await asyncio.sleep(random.uniform(0.4, 1.2))


async def fetch_offer(page, url: str, outdir: Path):
    try:
        resp = await page.goto(url, timeout=30000)
        html = await page.content()
        fname = re.sub(r"[^a-zA-Z0-9]", "_", url.strip("/"))[-100:] + ".html"
        outpath = outdir / fname
        outpath.write_text(html, encoding="utf-8")
        print(f"[OK] {url}")
    except Exception as e:
        print(f"[ERR] {url} -> {e}")
    await adaptive_sleep()


@dataclass
class Args:
    mode: str
    typ: str
    lokalizacje: List[str]
    max_strony: int
    raw_html_dir: Path
    concurrency: int

async def run_scrape(args: Args):
    args.raw_html_dir.mkdir(parents=True, exist_ok=True)
    offer_urls = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)


        for lok in args.lokalizacje:
            for page_num in range(1, args.max_strony + 1):
                url = LIST_URL.format(mode=args.mode, typ=args.typ, lok=lok, page=page_num)
                context = await browser.new_context(user_agent=random.choice(UA_POOL), locale="pl-PL")
                page = await context.new_page()
                await stealth_async(page)
                try:
                    resp = await page.goto(url, timeout=30000)
                    html = await page.content()
                    urls = await extract_offer_urls(html)
                    if not urls:
                        break
                    offer_urls.update(urls)
                    print(f"[PAGE {page_num}] {len(urls)} ofert ({lok})")
                except Exception as e:
                    print(f"[WARN] {url}: {e}")
                await page.close()
                await context.close()
                await adaptive_sleep()

        print(f"Znaleziono {len(offer_urls)} ofert do pobrania.")


        sem = asyncio.Semaphore(args.concurrency)

        async def worker(offer_url):
            async with sem:
                context = await browser.new_context(user_agent=random.choice(UA_POOL), locale="pl-PL")
                page = await context.new_page()
                await stealth_async(page)
                await fetch_offer(page, offer_url, args.raw_html_dir)
                await page.close()
                await context.close()

        await asyncio.gather(*[worker(u) for u in offer_urls])
        await browser.close()



if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Otodom fast HTML crawler")
    ap.add_argument("--mode", default="sprzedaz", choices=["sprzedaz", "wynajem"])
    ap.add_argument("--typ", default="mieszkanie")
    ap.add_argument("--lokalizacje", nargs="+", default=["cala-polska"])
    ap.add_argument("--max-strony", type=int, default=200)
    ap.add_argument("--raw-html", dest="raw_html", type=Path, required=True)
    ap.add_argument("--concurrency", type=int, default=5)
    args = ap.parse_args()

    a = Args(
        mode=args.mode,
        typ=args.typ,
        lokalizacje=args.lokalizacje,
        max_strony=args.max_strony,
        raw_html_dir=args.raw_html,
        concurrency=args.concurrency,
    )
    asyncio.run(run_scrape(a))
