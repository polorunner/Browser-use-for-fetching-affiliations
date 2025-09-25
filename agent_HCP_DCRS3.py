import asyncio
import os
import random
import logging
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd
import requests
import openpyxl
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from browser_use import Agent, Controller
from pydantic import BaseModel

# ------------------------------------------------------------------------
# CONFIGURATION & LOGGING
# ------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

# Dynamic filenames (can switch back if needed)
# date_str = datetime.now().strftime("%b%d_%Y")
# output_file = f"DCR_hcp_aff_{date_str}.xlsx"
# output_path = fr"C:\Projects\CYCLE\data\Data Change Requests- DCR\Merged_HCP_details_{date_str}.xlsx"

save_path = r"C:\Projects\ai-agent\exported files"
output_file = "Sep17_DCR_hcp_aff.xlsx"  # manual name for now
output_path = r'C:\Projects\CYCLE\data\Data Change Requests- DCR\DCR_28th_AUG\Merged_HCP_details_Sep17th_2.xlsx'

# ------------------------------------------------------------------------
# INPUT FILES
# ------------------------------------------------------------------------
dcrs_hcp = pd.read_excel(
    r"C:\Projects\CYCLE\data\Data Change Requests- DCR\DCR_17th_SEP\DCRs_hcp_list.xlsx",
    sheet_name="Sheet1"
)
dcrs_hcp.rename(columns={"Clinic ID": "Clinic_ID"}, inplace=True)

hcps_mainsail_df = pd.read_excel(
    r"C:\Projects\CYCLE\data\Data Change Requests- DCR\DCR_17th_SEP\mainsail_hcps.xlsx",
    sheet_name="Sheet1"
)

# ------------------------------------------------------------------------
# STEP 1 - PREPARE DATA
# ------------------------------------------------------------------------
def Create_Browser_use_dataset(dcrs_hcp: pd.DataFrame, hcps_mainsail_df: pd.DataFrame) -> pd.DataFrame:
    return (
        dcrs_hcp.merge(
            hcps_mainsail_df,
            how="left",
            left_on="Clinic_ID",
            right_on="customerid"
        )
        [["npi", "first_name", "last_name", "specialty", "city", "state"]]
        .assign(provider_name=lambda df: df["first_name"].astype(str) + " " + df["last_name"].astype(str))
    )

DCR_ready_hcps = Create_Browser_use_dataset(dcrs_hcp, hcps_mainsail_df)[
    ["npi", "provider_name", "specialty", "city", "state"]
]

# ------------------------------------------------------------------------
# STEP 2 - BROWSER AUTOMATION WITH CAPTCHA FALLBACK
# ------------------------------------------------------------------------

class Location(BaseModel):
    location_name: str
    address: str
    city: str
    state: str

class Doctor(BaseModel):
    provider_name: str
    specialty: str
    locations: List[Location]

class Doctors(BaseModel):
    doctors: List[Doctor]

controller = Controller(output_model=Doctors)

# Rotate between different browsers
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:118.0) Gecko/20100101 Firefox/118.0",
]

llm = ChatOpenAI(
    model="gpt-4o",
    temperature=0,
    api_key=os.getenv("OPENAI_API_KEY"),
    base_url=os.getenv("OPENAI_ENDPOINT"),
)

def is_captcha(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    return any(kw in lowered for kw in ["captcha", "verify you are human", "not a robot"])

async def fetch_doctor_locations(provider_name: str, specialty: str, city: str, state: str, npi: str) -> Dict[str, Any]:
    """Fetch locations with site fallback and general search. Always returns a row with placeholders if nothing found."""
    sources = ["vitals.com", "webmd.com", "healthgrades.com"]

    doctor_data = {
        "npi": npi,
        "doctor_name": provider_name,
        "specialty": specialty,
        "city": city,
        "state": state,
        "locations": [],
        "addresses": []
    }

    # --- Try site-specific sources ---
    for site in sources:
        task = f"""
        Find the practice location names and addresses of the healthcare provider:
        - Name: {provider_name}
        - Specialty: {specialty}
        - City: {city}
        - State: {state}

        Steps:
        1. Search "{provider_name} {specialty} {city} {state} site:{site}".
        2. Click the provider profile link.
        3. Navigate to the Locations tab or equivalent.
        4. Collect all practice locations and addresses.
        5. If blocked by CAPTCHA, skip to the next site.
        """

        agent = Agent(
            task=task,
            llm=llm,
            controller=controller,
            browser_config={"user_agent": random.choice(USER_AGENTS)}
        )

        try:
            history = await agent.run()
            result = history.final_result()
            logging.debug(f"🔎 Raw result for {provider_name} from {site}: {result}")

            if not result or is_captcha(result):
                logging.warning(f"⚠️ CAPTCHA/No data on {site} for {provider_name}, trying next...")
                continue

            parsed: Doctors = Doctors.model_validate_json(result)
            if parsed.doctors:
                for doctor in parsed.doctors:
                    doctor_data["locations"] = [loc.location_name for loc in doctor.locations]
                    doctor_data["addresses"] = [loc.address for loc in doctor.locations]

            if doctor_data["locations"]:
                logging.info(f"✅ Fetched {provider_name} from {site}: {doctor_data['locations']}")
                return {"doctors": [doctor_data]}

        except Exception as e:
            logging.error(f"❌ Error on {site} for {provider_name}: {e}")
            continue

    # --- General Google search fallback ---
    logging.warning(f"⚠️ No results on vitals/webmd/healthgrades for {provider_name}, trying general search...")

    task = f"""
    Find the practice location names and addresses of the healthcare provider:
    - Name: {provider_name}
    - Specialty: {specialty}
    - City: {city}
    - State: {state}

    Steps:
    1. Search "{provider_name} {specialty} {city} {state}" on Google.
    2. Click the most relevant result (hospital, clinic, provider page).
    3. Look for 'Locations' or 'Affiliations' section.
    4. Collect all practice locations and addresses.
    """

    agent = Agent(
        task=task,
        llm=llm,
        controller=controller,
        browser_config={"user_agent": random.choice(USER_AGENTS)}
    )

    try:
        history = await agent.run()
        result = history.final_result()
        logging.debug(f"🔎 Raw result for {provider_name} from general search: {result}")

        if result and not is_captcha(result):
            parsed: Doctors = Doctors.model_validate_json(result)
            if parsed.doctors:
                for doctor in parsed.doctors:
                    doctor_data["locations"] = [loc.location_name for loc in doctor.locations]
                    doctor_data["addresses"] = [loc.address for loc in doctor.locations]

        if doctor_data["locations"]:
            logging.info(f"✅ Fetched {provider_name} via general search: {doctor_data['locations']}")
        else:
            logging.warning(f"⚠️ No locations found anywhere for {provider_name}")

    except Exception as e:
        logging.error(f"❌ General search failed for {provider_name}: {e}")

    # --- Ensure placeholders if still empty ---
    if not doctor_data["locations"]:
        doctor_data["locations"] = ["Not Found"]
        doctor_data["addresses"] = ["Not Found"]

    return {"doctors": [doctor_data]}


async def fetch_multiple_doctors(doctors_list: List[Dict[str, str]]):
    """Fetch multiple doctors concurrently and save incrementally."""
    full_filename = os.path.join(save_path, output_file)

    tasks = [fetch_doctor_locations(**doc) for doc in doctors_list]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_data = []
    for res in results:
        if isinstance(res, Exception):
            continue
        all_data.extend(res["doctors"])

    df = pd.DataFrame(all_data)

    # Ensure 'locations' and 'addresses' columns always exist
    # for col in ["locations", "addresses"]:
    #     if col not in df.columns:
    #         df[col] = [[] for _ in range(len(df))]

    # df = df.explode(["locations", "addresses"], ignore_index=True)

    # cols_order = ["npi", "doctor_name", "specialty", "city", "state", "locations", "addresses"]
    # df = df[cols_order]
    # Ensure all expected columns exist
    expected_cols = ["npi", "doctor_name", "specialty", "city", "state", "locations", "addresses"]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = [] if col in ["locations", "addresses"] else None

    # Explode safely
    df = df.explode(["locations", "addresses"], ignore_index=True)
    

    df.to_excel(full_filename, index=False, engine="openpyxl")
    logging.info(f"💾 Saved results to {full_filename}")

# ------------------------------------------------------------------------
# STEP 3 - NPI REGISTRY LOOKUP
# ------------------------------------------------------------------------
def fetch_npi_details(npi):
    url = "https://npiregistry.cms.hhs.gov/api/"
    params = {"number": npi, "version": "2.1"}

    try:
        response = requests.get(url, params=params, timeout=10)
    except Exception as e:
        return {"NPI": npi, "Provider Name": "Error", "Role": "Error", "Specialty": "Error", "Address": str(e)}

    if response.status_code != 200:
        return {"NPI": npi, "Provider Name": "Error", "Role": "Error", "Specialty": "Error", "Address": f"HTTP {response.status_code}"}

    data = response.json()
    if not data.get("results"):
        return {"NPI": npi, "Provider Name": "Not Found", "Role": "Not Found", "Specialty": "Not Found", "Address": "Not Found"}

    result = data["results"][0]
    basic_info = result.get("basic", {})
    provider_name = f"{basic_info.get('first_name', '')} {basic_info.get('last_name', '')}".strip() or basic_info.get("organization_name", "Unknown")
    role = basic_info.get("credential", "Unknown")

    taxonomy_list = result.get("taxonomies", [])
    primary_taxonomy = next((t for t in taxonomy_list if t.get("primary")), {})
    specialty = primary_taxonomy.get("desc", "Unknown")

    addresses = result.get("addresses", [])
    practice_address = next((addr for addr in addresses if addr.get("address_purpose") == "LOCATION"), {})
    address = f"{practice_address.get('address_1', '')}, {practice_address.get('city', '')}, {practice_address.get('state', '')} {practice_address.get('postal_code', '')}"

    return {"NPI": npi, "Provider Name": provider_name, "Role": role, "Specialty": specialty, "Address": address}

# ------------------------------------------------------------------------
# STEP 4 - FINAL MERGE
# ------------------------------------------------------------------------
def final_merged_dataset(Browser_use_results_df, hcps_mainsail_df, NPIdb_df):
    hcps_mainsail_df = hcps_mainsail_df.loc[hcps_mainsail_df["npi"].notna()]
    hcps_renamed = hcps_mainsail_df.rename(columns={
        "primaryclinicname": "CRM_Primaryclinicname",
        "address1": "CRM_Address1",
        "city": "CRM_City",
        "state": "CRM_State",
        "zip": "CRM_Zip",
        "role": "CRM_Role"
    })[["npi", "customerid", "CRM_Primaryclinicname", "CRM_Address1", "CRM_City", "CRM_State", "CRM_Zip", "CRM_Role"]]

    merged_1 = Browser_use_results_df.merge(hcps_renamed, how="left", left_on="NPI", right_on="npi").drop(columns=["npi"])
    final_df = merged_1.merge(NPIdb_df, how="left", on="NPI")

    return final_df

# ------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------
if __name__ == "__main__":
    # Run in batches if needed: adjust .iloc[start:end]
    doctors_list = DCR_ready_hcps.iloc[24:,].to_dict(orient="records")

    asyncio.run(fetch_multiple_doctors(doctors_list))

    Browser_use_results_df = pd.read_excel(os.path.join(save_path, output_file), engine="openpyxl").rename(columns={
        "npi": "NPI",
        "doctor_name": "provider_name",
        "specialty": "specialty",
        "city": "BU_city",
        "state": "BU_state",
        "locations": "BU_locations",
        "addresses": "BU_addresses"
    })

    results = [fetch_npi_details(npi) for npi in list(DCR_ready_hcps["npi"])]
    NPIdb_df = pd.DataFrame(results).rename(columns={
        "Provider Name": "NPIdb_Provider_Name",
        "Role": "NPIdb_Role",
        "Specialty": "NPIdb_Specialty",
        "Address": "NPIdb_Address"
    })

    merged_df = final_merged_dataset(Browser_use_results_df, hcps_mainsail_df, NPIdb_df)

    columns_list_merged = [
        "NPI", "customerid", "provider_name", "specialty",
        "BU_city", "BU_state", "BU_locations", "BU_addresses",
        "CRM_Primaryclinicname", "CRM_Address1", "CRM_City", "CRM_State",
        "CRM_Zip", "NPIdb_Provider_Name", "NPIdb_Role", "NPIdb_Specialty",
        "NPIdb_Address"
    ]
    merged_df = merged_df[columns_list_merged]

    merged_df.to_excel(output_path, index=False)
    logging.info(f"🎉 Pipeline complete. Final merged file: {output_path}")
