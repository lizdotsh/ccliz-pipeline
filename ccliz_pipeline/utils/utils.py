
import re
import unicodedata
punctuation_pattern = re.compile(r"[^\w\s]")
whitespace_pattern = re.compile(r"\s+")
number_pattern = re.compile(r"\d")

def normalize_text(s: str) -> str:
    """
    Preprocesses a string for tokenization
    Turns to lowercase, removes punctuation, normalizes whitespace, normalizes unicode
    """
    s = s.lower()
    s = punctuation_pattern.sub("", s)
    # Normalize whitespace
    s = whitespace_pattern.sub(" ", s).strip()
    
    s= number_pattern.sub("", s)

    # Normalize unicode
    s = unicodedata.normalize("NFD", s)

    return "".join([c for c in s if not unicodedata.combining(c)])
