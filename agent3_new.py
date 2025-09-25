# ------------------------------
# 📦 Imports
# ------------------------------
import os
import json
import random
import asyncio
import pandas as pd
from typing import List, Dict, Any
from playwright.async_api import async_playwright
from openai import OpenAI


# ------------------------------
# ⚙️ Config Parameters
# ------------------------------
SAVE_PATH = r"C:\Projects\ai-agent\exported files"
OUTPUT_FILE = "doctor_locations_new.xlsx"
FULL_FILENAME = os.path.join(SAVE_PATH, OUTPUT_FILE)

# Initialize OpenAI client
client = OpenAI(api_key="YOUR_OPENAI_API_KEY")


# ------------------------------
# 🌐 Scraping Functions
# ------------------------------
async def scrape_with_fallback(provider_name: str, specialty: str, city: str, state: str, npi: str) -> Dict[str, Any]:
    """Try scraping HCP info from Healthgrades → Vitals → WebMD → Google."""

    doctor_data = {
        "npi": npi,
        "doctor_name": provider_name,
        "specialty": specialty,
        "city": city,
        "state": state,
        "locations": []
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # use headful mode to avoid CAPTCHAs
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Safari/537.36"
        )
        page = await context.new_page()

        # Ordered fallbacks
        for site in ["healthgrades.com", "vitals.com", "webmd.com"]:
            success = await scrape_site(page, site, provider_name, specialty, city, state, doctor_data)
            if success:
                await browser.close()
                return {"doctors": [doctor_data]}

        # Last fallback → Google snippet
        print(f"⚠️ Falling back to Google snippet for {provider_name}...")
        google_data = await scrape_google_snippet(page, provider_name, specialty, city, state)
        if google_data:
            doctor_data["locations"] = google_data

        await browser.close()

    return {"doctors": [doctor_data]}


async def scrape_site(page, site, provider_name, specialty, city, state, doctor_data):
    """Generic scraper for healthgrades/vitals/webmd using Google search."""
    query = f"{provider_name} {specialty} {city} {state} site:{site}"
    await page.goto(f"https://www.google.com/search?q={query}")
    await page.wait_for_timeout(random.randint(2000, 4000))

    # Grab first result
    links = await page.query_selector_all("a")
    hcp_link = None
    for link in links:
        href = await link.get_attribute("href")
        if href and site in href:
            hcp_link = href
            break

    if not hcp_link:
        return False

    await page.goto(hcp_link)
    await page.wait_for_timeout(3000)

    html = await page.content()
    if "captcha" in html.lower():
        print(f"🚫 CAPTCHA at {site}, skipping...")
        return False

    # Use LLM to parse locations
    prompt = f"""
    Extract all practice locations from the HTML below.
    Return JSON list with fields: 'location_name' and 'address'.

    HTML:
    {html}
    """
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        response_format={ "type": "json_object" }
    )

    parsed = json.loads(response.choices[0].message["content"])
    locations = parsed.get("locations", [])

    if locations:
        doctor_data["locations"] = locations
        print(f"✅ Found locations for {doctor_data['doctor_name']} on {site}")
        return True

    return False


async def scrape_google_snippet(page, provider_name, specialty, city, state):
    """Scrape address snippets directly from Google if everything else fails."""
    query = f"{provider_name} {specialty} {city} {state}"
    await page.goto(f"https://www.google.com/search?q={query}")
    await page.wait_for_timeout(2000)

    snippets = await page.query_selector_all("span, div")
    results = []
    for snip in snippets[:50]:
        text = (await snip.inner_text()).strip()
        if any(x in text for x in ["St", "Ave", "Blvd", "Suite", "Road"]):
            results.append({"location_name": provider_name, "address": text})

    return results if results else None


# ------------------------------
# 📑 Multi-Doctor Runner
# ------------------------------
async def fetch_multiple_doctors(doctors_list: List[Dict[str, str]]):
    """Fetch locations for multiple doctors and save incrementally."""

    results = await asyncio.gather(*[
        scrape_with_fallback(
            provider_name=doctor["provider_name"],
            specialty=doctor["specialty"],
            city=doctor["city"],
            state=doctor["state"],
            npi=doctor["npi"]
        )
        for doctor in doctors_list
    ])

    # Flatten results
    final_data = []
    for res in results:
        for d in res["doctors"]:
            for loc in d["locations"]:
                final_data.append({
                    "npi": d["npi"],
                    "doctor_name": d["doctor_name"],
                    "specialty": d["specialty"],
                    "city": d["city"],
                    "state": d["state"],
                    "location": loc.get("location_name", ""),
                    "address": loc.get("address", "")
                })

    df = pd.DataFrame(final_data)

    # Save results
    if not os.path.exists(FULL_FILENAME):
        df.to_excel(FULL_FILENAME, index=False, engine="openpyxl")
    else:
        with pd.ExcelWriter(FULL_FILENAME, mode="a", if_sheet_exists="overlay", engine="openpyxl") as writer:
            df.to_excel(writer, index=False, header=False, startrow=writer.sheets["Sheet1"].max_row)

    print(f"💾 Saved {len(final_data)} rows to {FULL_FILENAME}")


# ------------------------------
# 🚀 Main Function
# ------------------------------
if __name__ == "__main__":
    doctors = [
        {"provider_name": "John Doe", "specialty": "Cardiology", "city": "Dallas", "state": "TX", "npi": "1234567890"},
        {"provider_name": "Jane Smith", "specialty": "Neurology", "city": "Austin", "state": "TX", "npi": "0987654321"}
    ]

    asyncio.run(fetch_multiple_doctors(doctors))
