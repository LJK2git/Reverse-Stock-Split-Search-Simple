#!/usr/bin/env python3
"""
Fast Yahoo Finance Stock Split Calendar Scraper using Playwright (async).
- Launches one Chromium instance.
- Runs multiple pages concurrently.
- Blocks images/fonts/stylesheets/analytics to speed up loads.
- Concurrency is fixed to 5 (no prompt).
- Detects reverse splits and prints symbol, company, payable date, ratio, and link.
"""

import asyncio
import re
from datetime import datetime, timedelta
from typing import List, Tuple, Set
from bs4 import BeautifulSoup
import sys

from playwright.async_api import async_playwright, Page, Browser, TimeoutError as PWTimeout

# ---------- URL builder ----------
def build_url(day_str: str) -> str:
    day = datetime.strptime(day_str, "%Y-%m-%d")
    next_day = (day + timedelta(days=1)).strftime("%Y-%m-%d")
    return (
        f"https://finance.yahoo.com/calendar/splits"
        f"?from={day_str}&to={next_day}&day={day_str}&offset=0&size=100"
    )

# ---------- Scrape single day ----------
# returns list of (symbol, link, company, payable_on, ratio_text, is_reverse)
async def scrape_day_page(browser: Browser, day_str: str, semaphore: asyncio.Semaphore) -> List[Tuple[str, str, str, str, str, bool]]:
    url = build_url(day_str)
    async with semaphore:
        page: Page = await browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )

        # Block unnecessary resources
        async def route_intercept(route):
            req = route.request
            if req.resource_type in ("image", "font", "stylesheet"):
                await route.abort()
            elif "google-analytics" in req.url or "analytics" in req.url:
                await route.abort()
            else:
                await route.continue_()

        await page.route("**/*", route_intercept)

        results: List[Tuple[str, str, str, str, str, bool]] = []
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            # Wait for either table rows or a no-results element
            try:
                await page.wait_for_selector("table tbody tr, .simpTblRow, a[data-test='quoteLink']", timeout=7000)
            except PWTimeout:
                # continue — page may show no results
                pass

            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")

            ratio_re = re.compile(r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)")
            date_re = re.compile(r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},\s*\d{4}", re.IGNORECASE)

            # Try to parse table rows first
            rows = soup.select("table tbody tr") or soup.select(".simpTblRow")
            seen = set()
            for tr in rows:
                # find symbol/link
                link = tr.find("a", {"data-test": "quoteLink"})
                if not link:
                    link = tr.find("a", href=True)
                if not link:
                    continue

                symbol = link.text.strip().upper()
                href = link.get("href", "")
                full_href = href if href.startswith("http") else "https://finance.yahoo.com" + href

                if not symbol or symbol in seen:
                    continue

                # attempt to find company and payable date in the same row
                company = ""
                payable_on = ""
                ratio_text = ""

                tds = tr.find_all("td")
                # Common Yahoo layout: Symbol | Company | Payable On | Optionable? | Ratio
                if tds:
                    # company often in second cell
                    if len(tds) >= 2:
                        company = tds[1].get_text(separator=" ", strip=True)
                    # payable date often in third cell
                    if len(tds) >= 3:
                        payable_on = tds[2].get_text(separator=" ", strip=True)
                    # ratio often in last cell
                    candidate = tds[-1].get_text(separator=" ", strip=True)
                    m_ratio = ratio_re.search(candidate)
                    if m_ratio:
                        ratio_text = m_ratio.group(0)

                # fallback: search row text for company, date, ratio
                row_text = tr.get_text(separator=" ", strip=True)
                if not company:
                    # try to extract company by removing symbol from row_text and trimming
                    company = row_text.replace(symbol, "").strip()
                    # if company is too long or contains ratio/date, try to isolate by splitting on date or ratio
                    m_date = date_re.search(company)
                    if m_date:
                        company = company[:m_date.start()].strip(" -|,")
                if not payable_on:
                    m_date = date_re.search(row_text)
                    if m_date:
                        payable_on = m_date.group(0)
                if not ratio_text:
                    m_ratio = ratio_re.search(row_text)
                    if m_ratio:
                        ratio_text = m_ratio.group(0)

                # final fallback: search whole page near symbol
                if not payable_on:
                    page_text = soup.get_text(separator=" ", strip=True)
                    pattern = re.compile(re.escape(symbol) + r".{0,80}?" + r"(" + date_re.pattern + r")", re.IGNORECASE)
                    m = pattern.search(page_text)
                    if m:
                        payable_on = m.group(1)

                # normalize ratio and detect reverse split
                is_reverse = False
                m_ratio = ratio_re.search(ratio_text)
                if m_ratio:
                    try:
                        a = float(m_ratio.group(1))
                        b = float(m_ratio.group(2))
                        is_reverse = a > b
                        ratio_text = f"{a:g} - {b:g}"
                    except Exception:
                        is_reverse = False

                results.append((symbol, full_href, company or "N/A", payable_on or day_str, ratio_text or "N/A", is_reverse))
                seen.add(symbol)

            # If no rows found, fallback to scanning links across page
            if not results:
                links = soup.find_all("a", {"data-test": "quoteLink"})
                for link in links:
                    symbol = link.text.strip().upper()
                    href = link.get("href", "")
                    full_href = href if href.startswith("http") else "https://finance.yahoo.com" + href
                    if not symbol or symbol in seen:
                        continue

                    # try to find company and ratio near the link element
                    parent = link.find_parent()
                    parent_text = parent.get_text(separator=" ", strip=True) if parent else ""
                    company = ""
                    payable_on = ""
                    ratio_text = ""

                    # attempt to parse parent text
                    parts = parent_text.split("  ")
                    if parts and len(parts) >= 2:
                        # heuristic: company name often follows symbol
                        company = parts[1].strip()
                    m_date = date_re.search(parent_text)
                    if m_date:
                        payable_on = m_date.group(0)
                    m_ratio = ratio_re.search(parent_text)
                    if m_ratio:
                        ratio_text = m_ratio.group(0)

                    # page-wide fallbacks
                    if not company:
                        # try to find a company cell near the link by searching siblings
                        sib = link.find_next_sibling()
                        if sib:
                            company = sib.get_text(separator=" ", strip=True)
                    if not payable_on:
                        m_page_date = date_re.search(soup.get_text(separator=" ", strip=True))
                        if m_page_date:
                            payable_on = m_page_date.group(0)
                    if not ratio_text:
                        m_page_ratio = ratio_re.search(soup.get_text(separator=" ", strip=True))
                        if m_page_ratio:
                            ratio_text = m_page_ratio.group(0)

                    is_reverse = False
                    m_ratio = ratio_re.search(ratio_text)
                    if m_ratio:
                        try:
                            a = float(m_ratio.group(1))
                            b = float(m_ratio.group(2))
                            is_reverse = a > b
                            ratio_text = f"{a:g} - {b:g}"
                        except Exception:
                            is_reverse = False

                    results.append((symbol, full_href, company or "N/A", payable_on or day_str, ratio_text or "N/A", is_reverse))
                    seen.add(symbol)

        except Exception:
            # keep it quiet and return whatever we have
            pass
        finally:
            try:
                await page.close()
            except Exception:
                pass

        return results

# ---------- Date helpers ----------
def get_week_range() -> Tuple[str, str]:
    today = datetime.today()
    # find next Sunday (Sunday as day 6 if Monday=0)
    days_ahead = (6 - today.weekday()) % 7
    sunday = today + timedelta(days=days_ahead)
    saturday = sunday + timedelta(days=6)
    return sunday.strftime("%Y-%m-%d"), saturday.strftime("%Y-%m-%d")

def days_between(start: str, end: str) -> List[str]:
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    return [(s + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((e - s).days + 1)]

# ---------- Main ----------
async def run(date_list: List[str], filter_by_length: bool, concurrency: int = 5):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        sem = asyncio.Semaphore(concurrency)
        tasks = [scrape_day_page(browser, d, sem) for d in date_list]
        all_day_results = await asyncio.gather(*tasks)

        await browser.close()

    # flatten and filter
    seen: Set[str] = set()
    all_results: List[Tuple[str, str, str, str, str, bool]] = []
    for day_res in all_day_results:
        for sym, href, company, payable_on, ratio, is_reverse in day_res:
            if filter_by_length and len(sym) not in (3, 4):
                continue
            if sym not in seen:
                seen.add(sym)
                all_results.append((sym, href, company, payable_on, ratio, is_reverse))

    return all_results

def main():
    FIXED_CONCURRENCY = 5

    print("=" * 50)
    print("  Fast Yahoo Finance Stock Split Calendar Scraper")
    print("=" * 50)

    print("\nDate options:")
    print("  1) Today")
    print("  2) Tomorrow")
    print("  3) Specific date")
    print("  4) This week (next Sun–Sat)")
    print("  5) Custom date range")
    choice = input("\nChoose [1-5]: ").strip()

    if choice == "1":
        today = datetime.today().strftime("%Y-%m-%d")
        date_list = [today]
    elif choice == "2":
        tomorrow = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
        date_list = [tomorrow]
    elif choice == "3":
        d = input("Enter date (YYYY-MM-DD): ").strip()
        date_list = [d]
    elif choice == "4":
        start, end = get_week_range()
        print(f"Scraping {start} → {end}")
        date_list = days_between(start, end)
    elif choice == "5":
        start = input("Start date (YYYY-MM-DD): ").strip()
        end   = input("End date   (YYYY-MM-DD): ").strip()
        date_list = days_between(start, end)
    else:
        print("Invalid choice. Exiting.")
        sys.exit(1)

    filter_ans = input("\nFilter to tickers with only 3–4 characters? (yes/no): ").strip().lower()
    filter_by_length = filter_ans == "yes"

    print(f"\nUsing fixed concurrency: {FIXED_CONCURRENCY} pages")
    print("\nLaunching browser and scraping...\n")
    results = asyncio.run(run(date_list, filter_by_length, concurrency=FIXED_CONCURRENCY))

    if not results:
        print("⚠️  No tickers found.")
        sys.exit(0)

    print("=" * 50)
    print(f"✅  {len(results)} ticker(s) found")
    print("=" * 50)
    print("\n── Tickers with company, payable date, ratio ──")
    for symbol, link, company, payable_on, ratio, is_reverse in results:
        flag = " (REVERSE)" if is_reverse else ""
        print(f"{symbol}{flag} — {company} — {payable_on} — {ratio} — {link}")

    # Print reverse splits separately
    reverse_list = [(sym, company, payable_on, ratio, href) for sym, href, company, payable_on, ratio, is_rev in [(s, h, c, p, r, rev) for s, h, c, p, r, rev in results] if any(True for s2, h2, c2, p2, r2, rev2 in results if s2 == sym and rev2)]
    # Build properly:
    reverse_list = []
    for sym, href, company, payable_on, ratio, is_rev in results:
        if is_rev:
            reverse_list.append((sym, company, payable_on, ratio, href))

    if reverse_list:
        print("\n── Reverse splits detected ──")
        for sym, company, payable_on, ratio, href in reverse_list:
            print(f"{sym} — {company} — {payable_on} — {ratio} — {href}")
    else:
        print("\nNo reverse splits detected.")

if __name__ == "__main__":
    main()
