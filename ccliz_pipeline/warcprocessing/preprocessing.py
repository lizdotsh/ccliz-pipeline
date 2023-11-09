import trafilatura as tf
from resiliparse.parse.encoding import bytes_to_str


def preprocess_raw_bytes(body: bytes) -> str:
    return tf.extract(
        body,
        # bytes_to_str(body),
        include_comments=False,
        include_tables=False,
        favor_precision=True,
        no_fallback=True,
    )


"""
we remove any document that does not contain between 50 and 100,000 words, or 
whose mean word length is outside the range of 3 to 10 characters; we remove any 
document with a symbol-to-word ratio greater than 0.1 for either the hash symbol or the 
ellipsis; and we remove any document with more than 90% of lines starting with a bullet 
point, or more than 30% ending with an ellipsis. We also require that 80% of words in a 
document contain at least one alphabetic character, and apply a "stop word" ﬁlter, to 
remove documents that do not contain at least two of the following English words: the,
be, to, of, and, that, have, with; this adequately deals with ostensibly English 
documents that contain no cohe
"""


def preprocessing_rules(s: str) -> bool:
    """returns true if passes rule"""
    if not s:
        return False
    num_words = len(s.split())
    if num_words < 50 or num_words > 100000:
        return False
    if len(s) / num_words < 3 or len(s) / num_words > 10:
        return False
    if s.count("#") / num_words > 0.1:
        return False
    if s.count("...") / num_words > 0.1:
        return False
    if s.count("•") / num_words > 0.9:
        return False
    if s.count("...") / num_words > 0.3:
        return False
    if sum([1 for w in s.split() if not w.isalpha()]) / len(s.split()) > 0.8:
        return False
    if (
        sum(
            [
                1
                for w in s.split()
                if w in ["the", "be", "to", "of", "and", "that", "have", "with"]
            ]
        )
        < 2
    ):
        return False
    return True
