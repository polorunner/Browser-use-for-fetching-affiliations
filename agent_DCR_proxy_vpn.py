import asyncio
import os
import pandas as pd
import requests
import itertools
import subprocess
import time
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from browser_use import Agent, Controller
from pydantic import BaseModel
from typing import List, Dict, Any

# ============================
# CONFIG
# ============================

output_file = "Sep17_DCR_hcp_aff_2nd.xlsx"
output_path = r'C:\Projects\CYCLE\data\Data Change Requests- DCR\DCR_28th_AUG\Merged_HCP_details_Sep17th_2nd.xlsx'

# --- Proxy list (replace with real ones) ---
PROXIES = [
    "http://123.45.67.89:8080",
    "http://98.76.54.32:8000",
    "http://11.22.33.44:9000"
]

proxy_pool = itertools.cycle(PROXIES)
proxy_counter = 0
current_proxy = next(proxy_pool)

# ============================
# VPN FUNCTIONS (Windows Example with rasdial)
# ============================

VPN_PROFILE = "MyVPNProfile"   # must match your Windows VPN connection name
VPN_USERNAME = "your_username"
VPN_PASSWORD = "your_password"

def connect_vpn():
    """Connect to VPN"""
    disconnect_vpn()
    try:
        subprocess.run(
            ["rasdial", VPN_PROFILE, VPN_USERNAME, VPN_PASSWORD],
            check=True
        )
        print(f"[VPN] Connected to {VPN_PROFILE}")
        time.sleep(5)  # allow connection to stabilize
    except subprocess.CalledProcessError as e:
        print(f"[VPN] Failed to connect: {e}")

def disconnect_vpn():
    """Disconnect VPN"""
    subprocess.run(["rasdial", VPN_PROFILE, "/disconnect"], check=False)
    print("[VPN] Disconnected")
    time.sleep(3)

# ============================
# STEP 1 - PREPARE DATA
# ============================

dcrs_hcp = pd.read_excel(r"C:\Projects\CYCLE\data\Data Change Requests- DCR\DCR_17th_SEP\DCRs_hcp_list.xlsx", sheet_name='Sheet1')
dcrs_hcp.rename(columns={'Clinic ID' : 'Clinic_ID'}, inplace=True)

hcps_mainsail_df = pd.read_excel(r"C:\Projects\CYCLE\data\Data Change Requests- DCR\DCR_17th_SEP\mainsail_hcps.xlsx", sheet_name='Sheet1')

def Create_Browser_use_dataset(dcrs_hcp: pd.DataFrame, hcps_mainsail_df: pd.DataFrame) -> pd.DataFrame:
    return (
        dcrs_hcp
        .merge(
            hcps_mainsail_df,
            how='left',
            left_on='Clinic_ID',
            right_on='customerid'
        )
        [['npi', 'first_name', 'last_name', 'specialty', 'city', 'state']]
        .assign(provider_name=lambda df: df['first_name'].astype(str) + ' ' + df['last_name'].astype(str))
    )

DCR_ready_hcps = Create_Browser_use_dataset(dcrs_hcp, hcps_mainsail_df)[['npi','provider_name','specialty','city','state']]

# ============================
# STEP 2 - BROWSER USE + PROXY + VPN ROTATION
# ============================

load_dotenv()

llm = ChatOpenAI(
    model="gpt-4o",
    temperature=0,
    max_tokens=None,
    timeout=None,
    max_retries=2,
    api_key=os.getenv("OPENAI_API_KEY"),
    base_url=os.getenv("OPENAI_ENDPOINT"),
)

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

async def fetch_doctor_locations(provider_name: str, specialty: str, city: str, state: str, npi: str, iteration: int) -> Dict[str, Any]:
    global proxy_counter, current_proxy

    # Rotate proxy every 10
    proxy_counter += 1
    if proxy_counter % 10 == 1:
        current_proxy = next(proxy_pool)
        print(f"[PROXY] Switching to proxy: {current_proxy}")

    # Rotate VPN every 50
    if iteration % 50 == 1:
        print("[VPN] Rotating VPN...")
        connect_vpn()

    # Apply proxy env
    os.environ["HTTP_PROXY"] = current_proxy
    os.environ["HTTPS_PROXY"] = current_proxy

    task = f"""
    Find the practice location names and addresses of the healthcare provider whose name -> {provider_name}, living in city -> {city}, state -> {state}, and their specialty -> {specialty}.
    """

    agent = Agent(task=task, llm=llm, controller=controller)
    history = await agent.run()
    result = history.final_result()

    doctor_data = {
        "npi": npi,
        "doctor_name": provider_name,
        "specialty": specialty,
        "city": city,
        "state": state,
        "locations": [],
        "addresses": []
    }

    if result:
        try:
            parsed: Doctors = Doctors.model_validate_json(result)
            if parsed.doctors:
                for doctor in parsed.doctors:
                    doctor_data["locations"] = [loc.location_name for loc in doctor.locations]
                    doctor_data["addresses"] = [loc.address for loc in doctor.locations]

            print(f"Fetched for {provider_name}: {doctor_data['locations']}")
        except Exception as e:
            print(f"Parsing error for {provider_name}: {e}")

    return {"doctors": [doctor_data]}

async def fetch_multiple_doctors(doctors_list: List[Dict[str, str]], output_file=output_file):
    save_path = r"C:\Projects\ai-agent\exported files"
    full_filename = os.path.join(save_path, output_file)
    
    for i, doctor in enumerate(doctors_list, start=1):
        result = await fetch_doctor_locations(
            provider_name=doctor["provider_name"],
            specialty=doctor["specialty"],
            city=doctor["city"],
            state=doctor["state"],
            npi=doctor["npi"],
            iteration=i
        )
        
        df = pd.DataFrame(result["doctors"])
        df = df.explode(["locations", "addresses"], ignore_index=True)
        cols_order = ["npi", "doctor_name", "specialty", "city", "state", "locations", "addresses"]
        df = df[cols_order]

        if not os.path.exists(full_filename):
            df.to_excel(full_filename, index=False, engine='openpyxl')
        else:
            with pd.ExcelWriter(full_filename, mode='a', if_sheet_exists='overlay', engine='openpyxl') as writer:
                df.to_excel(writer, index=False, header=False, startrow=writer.sheets["Sheet1"].max_row)
        
        print(f"Saved {doctor['provider_name']} to Excel")

# ============================
# RUN
# ============================

if __name__ == "__main__":
    doctors_list = DCR_ready_hcps[["npi","provider_name","specialty","city","state"]].iloc[:200].to_dict(orient="records")
    connect_vpn()  # start VPN
    asyncio.run(fetch_multiple_doctors(doctors_list))
    disconnect_vpn()  # cleanup
