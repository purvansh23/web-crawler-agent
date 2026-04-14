import re

class Matcher:
    def __init__(self):
        # We compile combinations of cross-docking variants.
        # \bcross[\s\-_]?dock(?:ing|ed|s|er)?\b will match "cross dock", "cross-docking", "crossdocking", "cross docked"
        self.primary_pattern = re.compile(
            r'\bcross[\s\-_]?dock(?:ing|ed|s|er)?\b'
            r'|\bcrossdock(?:ing|ed|s|er)?\b',
            re.IGNORECASE
        )
        
        # Optional secondary patterns for logging or expanded context
        self.secondary_pattern = re.compile(
            r'\btransload(?:ing|ed)?\b'
            r'|\bbreak[\s\-]?bulk\b'
            r'|\bhub[\s\-]distribution\b'
            r'|\bdistribution[\s\-]center[\s\-]transfer\b',
            re.IGNORECASE
        )

    def has_primary_match(self, text: str) -> bool:
        """
        Returns True if ANY primary keyword is found in the text.
        This serves as the deterministic gateway before AI validation.
        """
        if not text:
            return False
        return bool(self.primary_pattern.search(text))

    def extract_snippet(self, text: str, ctx_chars: int = 500) -> str:
        """
        If a match is found, extract a snippet around the match
        so we can pass just the context to the AI layer to save tokens.
        """
        match = self.primary_pattern.search(text)
        if not match:
            return text[:1000] # Fallback to beginning of page
        
        start = max(0, match.start() - ctx_chars)
        end = min(len(text), match.end() + ctx_chars)
        return text[start:end]
