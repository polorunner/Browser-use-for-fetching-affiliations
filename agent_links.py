import asyncio
import os
import pandas as pd
import openpyxl
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from browser_use import Agent, Controller
from pydantic import BaseModel
from typing import List, Dict, Any

# Load environment variables
load_dotenv()

# Initialize OpenAI LLM
llm = ChatOpenAI(
    model="gpt-4o",
    temperature=0,
    api_key=os.getenv("OPENAI_API_KEY"),
    base_url=os.getenv("OPENAI_ENDPOINT"),
)

# Define response structure
class DoctorURL(BaseModel):
    provider_name: str
    specialty: str
    urls: List[str]

class DoctorsURLs(BaseModel):
    doctors: List[DoctorURL]

# Initialize the controller
controller = Controller(output_model=DoctorsURLs)

async def fetch_doctor_urls(provider_name: str, specialty: str, city: str, state: str) -> Dict[str, Any]:
    """Fetch URLs where doctor details are found."""
    
    task = f"""Find the webpage link containing the practice locations of the healthcare provider whose name -> {provider_name}, living in city -> {city}, state -> {state}, and their specialty -> {specialty}.
    
            Here are the steps:
    
            1. Search '{provider_name} {specialty} {city} {state} site:healthgrades.com' in the address bar.
            2. Look for the Healthgrades link in the search results and click on it.
            3. If the page opens, navigate to the **‘Locations’** section.
            4. Copy the webpage link where the practice locations are listed.
            5. If no profile is found on Healthgrades, repeat the search with **Vitals.com**.
            6. If still no result, try **WebMD.com**.
            7. If none of these websites provide a location page, return 'None'.
            8. If a page does not load properly, refresh and try again before moving to the next step.
    
            REMEMBER:
            - **Step 4 is the most important.**
            - **Return only the link where practice locations are found.**
            - **Do NOT return Google Maps links.**
            - **Use only trusted healthcare sites: Healthgrades, Vitals, WebMD.**
            """

    agent = Agent(task=task, llm=llm, controller=controller)
    history = await agent.run()
    result = history.final_result()

    doctor_data = {
        "doctor_name": provider_name,
        "specialty": specialty,
        "city": city,
        "state": state,
        "urls": []
    }

    if result:
        try:
            parsed: DoctorsURLs = DoctorsURLs.model_validate_json(result)
            if parsed.doctors:
                for doctor in parsed.doctors:
                    doctor_data["urls"] = doctor.urls

            print(f"Fetched URLs for {provider_name}: {doctor_data['urls']}")  # Debug Print
        except Exception as e:
            print(f"Parsing error for {provider_name}: {e}")  # Debug Print

    return doctor_data

async def fetch_multiple_doctors(doctors_list: List[Dict[str, str]], output_file="doctor_urls.xlsx"):   # Output file here
    """Fetch URLs for multiple doctors and save incrementally."""
    
    save_path = r"C:\\Projects\\ai-agent\\web-ui"
    full_filename = os.path.join(save_path, output_file)
    
    for doctor in doctors_list:
        result = await fetch_doctor_urls(
            provider_name=doctor["provider_name"],
            specialty=doctor["specialty"],
            city=doctor["city"],
            state=doctor["state"]
        )
        
        df = pd.DataFrame([result])
        df = df.explode("urls", ignore_index=True)  # Expand lists into separate rows
        
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
    df = df[required_columns]  # range here
    df.columns = ["provider_name", "specialty", "city", "state"]
    return df.to_dict(orient="records")

# Example Usage
if __name__ == "__main__":
    input_file = r"C:\\Projects\\ai-agent\\web-ui\\doctor_list.xlsx"  # Input file here
    doctors_list = load_doctors_from_excel(input_file)
    asyncio.run(fetch_multiple_doctors(doctors_list))
