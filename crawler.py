
import os
import re
import time
import json
import random
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from playwright_stealth import stealth_sync




DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

UA_POOL = [
    DEFAULT_USER_AGENT,
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
]

ROTATE_CONTEXT_EVERY = 60
MAX_RETRIES = 4
BASE_BACKOFF = 2.0
BASE = "https://www.otodom.pl"

LIST_URL_TMPL = BASE + "/pl/oferty/{mode}/{typ}/{lok}?page={page}&viewType=listing"

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)




def create_context(browser):
    ua = random.choice(UA_POOL)
    context = browser.new_context(user_agent=ua, locale="pl-PL")
    page = context.new_page()
    try:
        stealth_sync(page)
        page.add_init_script("""() => {
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        }""")
    except Exception as e:
        logger.warning("Stealth init error: %s", e)
    return context, page


def goto_with_retries(page, url, timeout=30000):
    backoff = BASE_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = page.goto(url, timeout=timeout)
            html = page.content()
            status = resp.status if resp else 200
            return status, html
        except Exception as e:
            logger.warning("Attempt %d failed for %s: %s", attempt, url, e)
            time.sleep(backoff + random.random())
            backoff *= 2
    return None, None


def adaptive_sleep(last_status=None, smin=1.0, smax=2.5):
    if last_status in (403, 429):
        delay = random.uniform(10, 30)
        logger.info("Throttled (%s) — sleep %.1fs", last_status, delay)
        time.sleep(delay)
    else:
        time.sleep(random.uniform(smin, smax))


def extract_offer_urls(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/pl/oferta/" in href:
            if href.startswith("/"):
                href = BASE + href
            urls.add(href.split("?")[0])
    return list(urls)


def build_list_url(mode: str, typ: str, lok: str, page: int) -> str:
    return LIST_URL_TMPL.format(mode=mode, typ=typ, lok=lok, page=page)




@dataclass
class Args:
    mode: str
    typ: str
    lokalizacje: List[str]
    max_strony: int
    raw_html_dir: Path
    sleep_min: float
    sleep_max: float


def run_scrape(args: Args):
    visited_urls = set()
    failed_urls = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context, page = create_context(browser)
        requests_since_rotate = 0

        for lok in args.lokalizacje:
            for pg in range(1, args.max_strony + 1):
                list_url = build_list_url(args.mode, args.typ, lok, pg)
                logger.info("lista: %s", list_url)
                status, html_list = goto_with_retries(page, list_url)
                if not html_list:
                    logger.warning("brak danych z listy %s", list_url)
                    break

                offers = extract_offer_urls(html_list)
                if not offers:
                    logger.info("brak ofert")
                    break

                logger.info("znaleziono %d ofert", len(offers))

                for offer_url in offers:
                    if offer_url in visited_urls:
                        continue
                    visited_urls.add(offer_url)

                    if requests_since_rotate >= ROTATE_CONTEXT_EVERY:
                        try:
                            context.close()
                        except Exception:
                            pass
                        context, page = create_context(browser)
                        requests_since_rotate = 0

                    status, html = goto_with_retries(page, offer_url, timeout=30000)
                    if html is None:
                        failed_urls.append(offer_url)
                        adaptive_sleep(status, args.sleep_min, args.sleep_max)
                        continue

                    args.raw_html_dir.mkdir(parents=True, exist_ok=True)
                    fname = re.sub(r"[^a-zA-Z0-9]", "_", offer_url.strip("/"))[-100:] + ".html"
                    path = args.raw_html_dir / fname
                    path.write_text(html, encoding="utf-8")
                    logger.info("zapisano %s", path)

                    requests_since_rotate += 1
                    adaptive_sleep(status, args.sleep_min, args.sleep_max)

        browser.close()

    if failed_urls:
        (args.raw_html_dir / "failed_urls.txt").write_text("\n".join(failed_urls), encoding="utf-8")
        logger.info("zapisano listę nieudanych ofert (%d)", len(failed_urls))



if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Otodom scraper (tylko HTML)")
    ap.add_argument("--mode", default="sprzedaz", choices=["sprzedaz", "wynajem"])
    ap.add_argument("--typ", default="mieszkanie")
    ap.add_argument("--lokalizacje", nargs="+", default=["warszawa"])
    ap.add_argument("--max-strony", type=int, default=3)
    ap.add_argument("--raw-html", dest="raw_html", type=Path, required=True)
    ap.add_argument("--sleep-min", type=float, default=1.0)
    ap.add_argument("--sleep-max", type=float, default=2.5)
    args = ap.parse_args()

    a = Args(
        mode=args.mode,
        typ=args.typ,
        lokalizacje=args.lokalizacje,
        max_strony=args.max_strony,
        raw_html_dir=args.raw_html,
        sleep_min=args.sleep_min,
        sleep_max=args.sleep_max,
    )
    run_scrape(a)
