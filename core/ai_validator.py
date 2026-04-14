import json
import os
from dotenv import load_dotenv
import anthropic

# Load variables from .env file
load_dotenv()

class AIValidator:
    def __init__(self):
        self.api_key = os.environ.get("ANTHROPIC_API_KEY")
        self.client = anthropic.Anthropic(api_key=self.api_key) if self.api_key else None
        self.system_prompt = """You are verifying if a company OFFERS cross docking as a service.
Answer with strictly JSON mapping these keys:
{
    "offers_cross_docking": true/false,
    "confidence": "high/medium/low",
    "reason": "one sentence explanation"
}
Return false if the keyword appears only in: 
1. Customer testimonials about competitors
2. "We don't offer" or "we stopped offering" statements
3. Unrelated industry generic talk
4. Job postings (unless it clearly indicates it's for their core service facility)."""

    def validate(self, company_name: str, page_url: str, snippet: str) -> bool:
        """
        Calls the LLM to validate if the context of the keyword match implies they offer the service.
        Returns True if validated, False otherwise.
        """
        if not self.client:
            # Fallback mock validation if no API key is provided
            print(f"[MOCK AI] No API key. Auto-approving match for {company_name}")
            return True
            
        try:
            response = self.client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=200,
                system=self.system_prompt,
                messages=[{
                    "role": "user",
                    "content": f"Company: {company_name}\nPage URL: {page_url}\n\nPage text snippet:\n{snippet}"
                }]
            )
            
            result_json = response.content[0].text
            # Attempt to parse json, handling markdown block wrapping if LLM hallucinates it
            if "```json" in result_json:
                result_json = result_json.split("```json")[1].split("```")[0].strip()
            elif "```" in result_json:
                result_json = result_json.split("```")[1].strip()
                
            data = json.loads(result_json)
            return data.get("offers_cross_docking", False)
        except Exception as e:
            print(f"AI Validation Error for {company_name}: {str(e)}")
            # If AI fails, err on the side of caution? Or default True since Regex matched?
            # Let's default True because Regex matched, to avoid missing data due to API error.
            return True
