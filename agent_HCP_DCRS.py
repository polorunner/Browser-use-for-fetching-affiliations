import asyncio
import os
import pandas as pd
import requests
import openpyxl
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from browser_use import Agent, Controller
from pydantic import BaseModel
from typing import List, Dict, Any

output_file = "Sep17_DCR_hcp_aff_2nd.xlsx" # add dynamic updates
output_path = r'C:\Projects\CYCLE\data\Data Change Requests- DCR\DCR_28th_AUG\Merged_HCP_details_Sep17th_2nd.xlsx' # getting used in the end, change path every time

## STEP 1 - Prepare the data for browser use case 

# change file name
dcrs_hcp = pd.read_excel(r"C:\Projects\CYCLE\data\Data Change Requests- DCR\DCR_17th_SEP\DCRs_hcp_list.xlsx", sheet_name='Sheet1')  # change here every time you want to run it
dcrs_hcp.rename(columns={'Clinic ID' : 'Clinic_ID'}, inplace=True)

dcrs_hcp

# Latest extract of Mainsail hcps
hcps_mainsail_df = pd.read_excel(r"C:\Projects\CYCLE\data\Data Change Requests- DCR\DCR_17th_SEP\mainsail_hcps.xlsx", sheet_name='Sheet1') # change here every time you want to run it
hcps_mainsail_df

# create the dataset for browser-use
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

DCR_ready_hcps = Create_Browser_use_dataset(dcrs_hcp = dcrs_hcp, hcps_mainsail_df=hcps_mainsail_df)[['npi','provider_name', 'specialty', 'city', 'state']]
# DCR_ready_hcps.drop_duplicates(inplace=True)
# DCR_ready_hcps = DCR_ready_hcps.iloc[3:,:] # to be deleted for the next run 


## STEP 2 - BROWSER-USE MODIFICATION


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

async def fetch_doctor_locations(provider_name: str, specialty: str, city: str, state: str, npi: str) -> Dict[str, Any]:
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

            print(f"Fetched for {provider_name}: Locations - {doctor_data['locations']}, Addresses - {doctor_data['addresses']}")  # Debug Print
        except Exception as e:
            print(f"Parsing error for {provider_name}: {e}")  # Debug Print

    return {"doctors": [doctor_data]}  # Ensuring return value even if no data is found


async def fetch_multiple_doctors(doctors_list: List[Dict[str, str]], output_file=output_file):
    """Fetch locations for multiple doctors and save incrementally."""
    
    save_path = r"C:\Projects\ai-agent\exported files"
    full_filename = os.path.join(save_path, output_file)
    
    for doctor in doctors_list:
        result = await fetch_doctor_locations(
            provider_name=doctor["provider_name"],
            specialty=doctor["specialty"],
            city=doctor["city"],
            state=doctor["state"],
            npi=doctor["npi"]
        )
        
        df = pd.DataFrame(result["doctors"])
        df = df.explode(["locations", "addresses"], ignore_index=True)  # Expand lists into separate rows
        
        # Make sure npi is the first column in final output
        cols_order = ["npi", "doctor_name", "specialty", "city", "state", "locations", "addresses"]
        df = df[cols_order]

        if not os.path.exists(full_filename):
            df.to_excel(full_filename, index=False, engine='openpyxl')
        else:
            with pd.ExcelWriter(full_filename, mode='a', if_sheet_exists='overlay', engine='openpyxl') as writer:
                df.to_excel(writer, index=False, header=False, startrow=writer.sheets["Sheet1"].max_row)
        
        print(f"Saved {doctor['provider_name']} to Excel")  # Debug print

# Load doctors from Excel
# def load_doctors_from_excel(file_path: str) -> List[Dict[str, str]]:
#     """Load HCP data from an Excel file and convert it to a list of dictionaries."""
#     df = pd.read_excel(file_path, engine='openpyxl', sheet_name='Sheet1')
#     required_columns = ["npi", "provider_name", "specialty", "city", "state"]
#     df = df[required_columns].loc[:, :]
#     df.columns = ["npi", "provider_name", "specialty", "city", "state"]
#     return df.to_dict(orient="records")



# Usage
if __name__ == "__main__":
    
    doctors_list = DCR_ready_hcps[["npi", "provider_name", "specialty", "city", "state"]].iloc[55:,:].to_dict(orient="records")  # delete .loc segment , this is for batchwise
    asyncio.run(fetch_multiple_doctors(doctors_list))

# Additional preprocessing steps
Browser_use_results_df = pd.read_excel(os.path.join(r"C:\Projects\ai-agent\exported files", output_file), engine='openpyxl')
Browser_use_results_df.rename(columns={
    'npi'        : 'NPI',
    'doctor_name': 'provider_name',
    'specialty'  : 'specialty',
    'city'       : 'BU_city',
    'state'      : 'BU_state',
    'locations'  : 'BU_locations',
    'addresses'  : 'BU_addresses'
}, inplace=True)



## STEP 3 - API call to NPIDB to fetch hcp details


def fetch_npi_details(npi):
    url = "https://npiregistry.cms.hhs.gov/api/"
    params = {
        "number": npi,
        "version": "2.1"
    }

    response = requests.get(url, params=params)  # API calling
    if response.status_code != 200:
        return {
            "NPI": npi,
            "Provider Name": "Error",
            "Role": "Error",
            "Specialty": "Error",
            "Address": f"HTTP {response.status_code}"
        }

    data = response.json() #json parsing
    if not data.get("results"):
        return {
            "NPI": npi,
            "Provider Name": "Not Found",
            "Role": "Not Found",
            "Specialty": "Not Found",
            "Address": "Not Found"
        }

    result = data["results"][0]

    # 1. Extract Provider Name
    basic_info = result.get("basic", {})
    provider_name = f"{basic_info.get('first_name', '')} {basic_info.get('last_name', '')}".strip()
    if not provider_name:
        provider_name = basic_info.get("organization_name", "Unknown")

    # 2. Extract Role (Credentials)
    role = basic_info.get("credential", "Unknown")

    # 3. Extract Specialty
    taxonomy_list = result.get("taxonomies", [])
    primary_taxonomy = next((t for t in taxonomy_list if t.get("primary")), {})  #doing this because only each description of primary will be true, uses a generator instead of a list
    specialty = primary_taxonomy.get("desc", "Unknown")

    # 4. Extract Address
    addresses = result.get("addresses", [])
    practice_address = next((addr for addr in addresses if addr.get("address_purpose") == "LOCATION"), {})
    address = f"{practice_address.get('address_1', '')}, {practice_address.get('city', '')}, {practice_address.get('state', '')} {practice_address.get('postal_code', '')}"

    return {
        "NPI": npi,
        "Provider Name": provider_name,
        "Role": role,
        "Specialty": specialty,
        "Address": address
    }


npi_list = list(DCR_ready_hcps['npi'])
# Loop through each NPI and collect details
results = []
for npi in npi_list:
    details = fetch_npi_details(npi)
    results.append(details)

# Convert to DataFrame
NPIdb_df = pd.DataFrame(results).rename(columns={
            'Provider Name': 'NPIdb_Provider_Name',
            'Role'         : 'NPIdb_Role',
            'Specialty'    : 'NPIdb_Specialty',
            'Address'      : 'NPIdb_Address'
        })



# STEP 4 - Merge everything

hcps_mainsail_df = hcps_mainsail_df.loc[hcps_mainsail_df['npi'].notna()]  # remove null NPIs
columns_list = ['npi', 'customerid', 'first_name', 'last_name', 'role', 'specialty', 'primaryclinicname', 'address1', 'city', 'state', 'zip']
hcps_mainsail_df[columns_list]


def final_merged_dataset(
    Browser_use_results_df,
    hcps_mainsail_df,
    NPIdb_df
):
    
    # Step 1: Select and rename relevant columns from hcps_mainsail_df
    hcps_renamed = hcps_mainsail_df.rename(columns={
        'primaryclinicname': 'CRM_Primaryclinicname',
        'address1'  : 'CRM_Address1',
        'city'      : 'CRM_City',
        'state'     : 'CRM_State',
        'zip'       : 'CRM_Zip',
        'role'      : 'CRM_Role'
    })[['npi', 'customerid', 'CRM_Primaryclinicname', 'CRM_Address1', 'CRM_City', 'CRM_State', 'CRM_Zip', 'CRM_Role']]
    
    # Step 2: First left join: Browser_use_results_df + hcps_renamed
    merged_1 = Browser_use_results_df.merge(
        hcps_renamed,
        how='left',
        left_on='NPI',
        right_on='npi'
    ).drop(columns=['npi'])  
    
    # Step 3: Second left join: merged_1 + NPIdb_df
    final_df = merged_1.merge(
        NPIdb_df,
        how='left',
        on='NPI'
    )
    
    return final_df



merged_df = final_merged_dataset(Browser_use_results_df, hcps_mainsail_df, NPIdb_df)


# STEP 5 - Rearrange and export, change path
columns_list_merged = [
    'NPI', 'customerid', 'provider_name', 'specialty',
    'BU_city', 'BU_state', 'BU_locations', 'BU_addresses',
    'CRM_Primaryclinicname', 'CRM_Address1', 'CRM_City', 'CRM_State',
    'CRM_Zip', 'NPIdb_Provider_Name', 'NPIdb_Role', 'NPIdb_Specialty',
    'NPIdb_Address'
]

merged_df = merged_df[columns_list_merged]

merged_df.to_excel(output_path, index=False)

# message for completion
print("All of the tasks have been completed successfully, please check the excel file!")