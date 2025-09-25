import asyncio
import os
import pandas as pd
import openpyxl
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from browser_use import Agent
from browser_use import Controller
from pydantic import BaseModel
from typing import List, Dict, Any

load_dotenv()

# Initialize OpenAI LLM
llm = ChatOpenAI(
    model="gpt-4o",
    temperature=0,
    max_tokens=None,
    timeout=None,
    max_retries=2,
    api_key=os.getenv("OPENAI_API_KEY"),
    base_url=os.getenv("OPENAI_ENDPOINT"),
)

# Define response structure
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

# Initialize the controller
controller = Controller(output_model=Doctors)

async def fetch_doctor_locations(provider_name: str, specialty: str, city: str, state: str) -> Dict[str, Any]:
    """Fetch locations for a given doctor and return as a dictionary."""
    
    task = f"""Find the practice location names and addresses of the healthcare provider whose name -> {provider_name}, living in city -> {city}, state -> {state}, and their specialty -> {specialty}.

        Here are the steps:

        1. Search '{provider_name} {specialty} {city} {state} site:vitals.com' in the address bar.
        2. You will get a Google search page. Find the vitals link on the search page and click on it.
        3. The page should open an HCP profile with HCP information. If it does not, go back to the search result and find the next vitals.com result link.
        4. Scroll down slightly, find the 'Locations' tab, and click on it.
        5. You should now see all the doctor's practice locations.
        6. Fetch all the location names and addresses of the doctor.
        7. If no match is found for a particular HCP in the first 120 seconds, return none.
        8. If vitals.com stops working midway, refresh the page and try again before checking other websites.
        9. If you are stuck in CAPTCHA, then search on webmd.com.
        10. Alternate between vitals.com , webmd.com, healthgrades.com if you find CAPTCHA.

        REMEMBER:
        - Step 6 is very important.
        - Wait for each element to load before interacting.
        - If no match is found on vitals.com, search on either webmd.com or healthgrades.com and follow the same process.
        - Do a Google search if unable to find on either vitals.com or webmd.com and click on the top link.
        """
        
        
    # task = f"""Find the practice location names and addresses of the healthcare provider whose name -> {provider_name}, living in city -> {city}, state -> {state}, and their specialty -> {specialty}.

    #     Here are the steps:

    #     1. Search '{provider_name} {specialty} {city} {state} site:healthgrades.com' in the address bar.
    #     2. You will get a Google search page. Find the Healthgrades link on the search page and click on it.
    #     3. The page should open an HCP profile with HCP information. If it does not, go back to the search result and find the next Healthgrades.com result link.
    #     4. Scroll down slightly, find the 'Locations' tab, and click on it.
    #     5. You should now see all the doctor's practice locations.
    #     6. Fetch all the location names and addresses of the doctor.
    #     7. If no match is found for a particular HCP in the first 120 seconds, return none.
    #     8. If Healthgrades.com stops working midway, refresh the page and try again before checking other websites.
    #     9. If you are stuck in CAPTCHA, then search on vitals.com.
    #     10. Alternate between vitals.com , webmd.com, healthgrades.com if you find CAPTCHA.

    #     REMEMBER:
    #     - Step 6 is very important.
    #     - Wait for each element to load before interacting.
    #     - If no match is found on Healthgrades.com, search on Vitals.com and follow the same process.
    #     - Do a Google search if unable to find on either Healthgrades.com or Vitals.com and click on the top link.
    #     """

    agent = Agent(task=task, llm=llm, controller=controller)
    history = await agent.run()
    result = history.final_result()

    doctor_data = {
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

            print(f"Fetched for {provider_name}: Locations - {doctor_data['locations']}, Addresses - {doctor_data['addresses']}")  # Debug Print
        except Exception as e:
            print(f"Parsing error for {provider_name}: {e}")  # Debug Print

    return {"doctors": [doctor_data]}  # Ensuring return value even if no data is found

async def fetch_multiple_doctors(doctors_list: List[Dict[str, str]], output_file="hcp_locations_Sajazir_V0.2.xlsx"):
    """Fetch locations for multiple doctors and save incrementally."""
    
    save_path = r"C:\Projects\ai-agent\web-ui"
    full_filename = os.path.join(save_path, output_file)
    
    for doctor in doctors_list:
        result = await fetch_doctor_locations(
            provider_name=doctor["provider_name"],
            specialty=doctor["specialty"],
            city=doctor["city"],
            state=doctor["state"]
        )
        
        df = pd.DataFrame(result["doctors"])
        df = df.explode(["locations", "addresses"], ignore_index=True)  # Expand lists into separate rows
        
        if not os.path.exists(full_filename):
            df.to_excel(full_filename, index=False, engine='openpyxl')
        else:
            with pd.ExcelWriter(full_filename, mode='a', if_sheet_exists='overlay', engine='openpyxl') as writer:
                df.to_excel(writer, index=False, header=False, startrow=writer.sheets["Sheet1"].max_row)
        
        print(f"Saved {doctor['provider_name']} to Excel")  # Debug print

# Load doctors from Excel
def load_doctors_from_excel(file_path: str) -> List[Dict[str, str]]:
    """Load HCP data from an Excel file and convert it to a list of dictionaries."""
    df = pd.read_excel(file_path, engine='openpyxl', sheet_name='Sheet1')
    required_columns = ["provider_name", "specialty", "city", "state"]
    df = df[required_columns].loc[133: , :]   # Fetching range
    df.columns = ["provider_name", "specialty", "city", "state"]
    return df.to_dict(orient="records")

# Example Usage
if __name__ == "__main__":
    input_file = r"C:\Projects\ai-agent\web-ui\Sajazir_HCPS_for_use.xlsx"
    doctors_list = load_doctors_from_excel(input_file)
    asyncio.run(fetch_multiple_doctors(doctors_list))


























