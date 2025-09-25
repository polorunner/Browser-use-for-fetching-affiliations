import os
from langchain_openai import ChatOpenAI
from browser_use import Agent
import asyncio
from dotenv import load_dotenv , find_dotenv
load_dotenv()


llm = ChatOpenAI(
    model="gpt-4o",
    temperature=0,
    max_tokens=None,
    timeout=None,
    max_retries=2,
    api_key= os.getenv("OPENAI_API_KEY"),  # if you prefer to pass api key in directly instaed of using env vars
    base_url=os.getenv("OPENAI_ENDPOINT"),
    # organization="...",
)
async def main():
    task = "Give me top 5 movies list by revenue. "
    agent = Agent(task=task, llm=llm)
    result = await agent.run()
    print(result)

asyncio.run(main())