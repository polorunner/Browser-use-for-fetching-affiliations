import asyncio
import os
import pandas as pd
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
    
    # task = f"""Find the practice location names and addresses of the healthcare provider whose name -> {provider_name}, living in city -> {city}, state -> {state},and their specialty -> {specialty} .

    #     Here are the steps:

    #     1. Search '{provider_name} {specialty} {city} {state} site:healthgrades.com' in the address bar 
    #     2. You will get a google search page. Find the healthgrades link on the search page and click on it
    #     3. The page should open a HCP profile with HCP information on it. If it does not, go back to search result and find the next healthgrades.com result link.
    #     4. Scroll a little, find the 'Locations' tab and click on it.
    #     5. You would now see all of the doctors practice locations.
    #     7. Fetch all the location names and the location addresses of the doctor as there could be MULTIPLE LOCATIONS !!
    #     8. Make sure to fetch each and every practice location and address of the doctor.
    #     9. The location name may be represented by html tag a.visit-practice-link.hg-track or span.visit-practice-link.hg-track.
    #     10. The addresses name may be represented by span.street-address 
        


    #     REMEMBER:
    #     - Step 8 is very important.
    #     - Wait for each element to load before interacting
    #     - If no match found in healthgrades.com , then search on vitals.com and do the same task.
    #     - Do a google search if unable to find on either healthgrades.com or vitals.com and click on the top link.
    #     """
    
    task = f"""Find the location name and address of healthcare provider whose name -> {provider_name}, living in city -> {city}, state -> {state},and their specialty -> {specialty} .

        Here are the steps:

        1. Search '{provider_name} {specialty} {city} {state} site:healthgrades.com' in the address bar 
        2. You will get a google search page. Find the healthgrades link on the search page and click on it
        3. The page should open a HCP profile with HCP information on it. If it does not, go back to search result and find the next healthgrades.com result link.
        4. Scroll a little, find the 'Locations' tab and click on it.
        5. You would now see all of the doctors location.
        7. Fetch all the location names and addresses of the HCP.
        8. A HCP could sit at multiple locations. Fetch every location name and address.
        9. Do this for all of the locations and addresses for a HCP.


        REMEMBER:
        - Step 7, 8 and 9 are very important.
        - Wait for each element to load before interacting
        - If no match found in healthgrades.com , then search on vitals.com and do the same task.
        - Do a google search if unable to find on either healthgrades.com or vitals.com and click on the top link.
        """

    agent = Agent(task=task, llm=llm, controller=controller)
    history = await agent.run()
    result = history.final_result()

    if result:
        parsed: Doctors = Doctors.model_validate_json(result)
        doctor_data = []
        for doctor in parsed.doctors:
            doctor_data.append({
                "doctor_name": doctor.provider_name,
                "specialty": doctor.specialty,
                "city": doctor.locations[0].city if doctor.locations else None,
                "state": doctor.locations[0].state if doctor.locations else None,
                "locations": [loc.location_name for loc in doctor.locations],
                "addresses": [loc.address for loc in doctor.locations]
            })
        return {"doctors": doctor_data}
    
    return {"doctors": []}  # Return empty if no result found

async def fetch_multiple_doctors(doctors_list: List[Dict[str, str]]) -> Dict[str, Any]:
    """Fetch locations for multiple doctors and return results as a dictionary."""
    all_results = {"doctors": []}

    for doctor in doctors_list:
        result = await fetch_doctor_locations(
            provider_name=doctor["provider_name"],
            specialty=doctor["specialty"],
            city=doctor["city"],
            state=doctor["state"]
        )
        all_results["doctors"].extend(result["doctors"])  # Append results

    return all_results

def convert_to_dataframe(results: Dict[str, Any]) -> pd.DataFrame:
    """Convert results dictionary to a Pandas DataFrame."""
    df = pd.DataFrame(results["doctors"])

    # Explode 'locations' and 'addresses' into separate rows
    df = df.explode(["locations", "addresses"], ignore_index=True)

    return df

def save_to_excel(df: pd.DataFrame, filename="doctor_results.xlsx"):
    """Save the DataFrame to an Excel file."""
    save_path = r"C:\Projects\ai-agent\exported files"
    full_filename = os.path.join(save_path, filename)
    df.to_excel(full_filename, index=False, engine='openpyxl')
    print(f"Results saved to {filename}")

# Example Usage
if __name__ == "__main__":
    doctors_list = [
        {"provider_name": "Jeremy Katcher", "specialty": "Allergy/Immunology", "city": "Saint Louis", "state": "MO"}
        
    ]

    results = asyncio.run(fetch_multiple_doctors(doctors_list))
    df = convert_to_dataframe(results)  # Convert JSON to DataFrame
    print(df)  # Display DataFrame
    save_to_excel(df)  # Save results to an Excel file
