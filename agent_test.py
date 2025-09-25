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
class pi(BaseModel):
    pi: str

class url(BaseModel):
    url_link: str
    nct_id: str
    pi: List[str]

class urls(BaseModel):
    urls: List[url]

# Initialize the controller
controller = Controller(output_model=urls)

async def fetch_pi_name() -> Dict[str, Any]:
    """Fetch Names of Principal Investigator of the Clinical Trials"""

    #  {url_link} or directly visit: 
    
    task = f"""Who won champions trophy cricket in the year 2025 ?"""

    agent = Agent(task=task, llm=llm, controller=controller)
    history = await agent.run()
    result = history.final_result()

    if result:
        return result
    
    return None# Ensuring return value even if no data is found

async def fetch_pi_names(urls_list: List[Dict[str, str]], output_file="PI_CT_AML_V0.1.xlsx"):
    """Fetch PI names for multiple CTs and save incrementally."""
    
    save_path = "C:\\Users\\ake.kowsik\\OneDrive - Beghou Consulting, LLC\\Documents\\Servier\\web-ui"
    full_filename = os.path.join(save_path, output_file)
    
    for url in urls_list:
        result = await fetch_pi_name(
            url_link = url["Study URL"],
            nct_id = url["NCT Number"],
        )
        
        df = pd.DataFrame(result["urls"])
        df = df.explode(["pi"], ignore_index=True)  # Expand lists into separate rows
        
        if not os.path.exists(full_filename):
            df.to_excel(full_filename, index=False, engine='openpyxl')
        else:
            with pd.ExcelWriter(full_filename, mode='a', if_sheet_exists='overlay', engine='openpyxl') as writer:
                df.to_excel(writer, index=False, header=False, startrow=writer.sheets["Sheet1"].max_row)
        
        print(f"Saved {url['NCT Number']} to Excel")  # Debug print

# Load doctors from Excel
def load_urls_from_excel(file_path: str) -> List[Dict[str, str]]:
    """Load HCP data from an Excel file and convert it to a list of dictionaries."""
    df = pd.read_excel(file_path, engine='openpyxl', sheet_name='AML')
    required_columns = ["NCT Number", "Study URL"]
    df = df[required_columns].loc[:2 , :]   # Fetching range
    df.columns = ["NCT Number", "Study URL"]
    print(df)
    return df.to_dict(orient="records")

# Example Usage
if __name__ == "__main__":

    asyncio.run(fetch_pi_name())


























