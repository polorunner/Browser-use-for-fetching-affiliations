import asyncio
import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from browser_use import Agent
from browser_use import Controller
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

    task = f"""Find the webpage links containing the practice locations of the healthcare provider whose name -> {provider_name}, living in city -> {city}, state -> {state}, and their specialty -> {specialty}.

            Here are the steps:

            1. Search '{provider_name} {specialty} {city} {state} site:healthgrades.com' in the address bar.
            2. Click on the Healthgrades link from the search results.
            3. Navigate to the **‘Locations’** section.
            4. There could be multiple locations for a hcp. Individually, we will handle the case for each location by the steps ahead.
            5. For each practice location:
            - If the location name is **clickable**, copy the link of that specific location.
            - If the location name is **not clickable**, copy the webpage URL where you found the location of that hcp.
            6. If no profile is found on Healthgrades, repeat the search with **Vitals.com**.
            7. If searching on **Vitals.com or WebMD.com**, copy the webpage link where all locations are listed.
            - If multiple locations are found on the same page, **return only that one page’s URL** (do not duplicate).
            8. If none of these websites provide a location page, return 'None'.
            9. If a page does not load properly, refresh and try again before moving to the next step.

            REMEMBER:
            - There could be multiple locations for a hcp. Fetch the links for each and every address.
            - **Step 5 is CRITICAL for Healthgrades.**
            - **For WebMD and Vitals, return the webpage URL listing all locations.**
            - **Do NOT return Google Maps links.**
            - **Only use trusted healthcare sites: Healthgrades, Vitals, WebMD.**
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

# Run for a single doctor
if __name__ == "__main__":
    single_hcp = {
        "provider_name": "Laura Green",
        "specialty": "Allergy/Immunology",
        "city": "Knoxville",
        "state": "TN"
    }

    result = asyncio.run(fetch_doctor_urls(
        provider_name=single_hcp["provider_name"],
        specialty=single_hcp["specialty"],
        city=single_hcp["city"],
        state=single_hcp["state"]
    ))

    print("\nFinal Output:", result)
