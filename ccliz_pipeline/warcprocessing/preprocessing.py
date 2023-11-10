import re

import trafilatura as tf
from resiliparse.parse.encoding import bytes_to_str

hashtag_line_re = re.compile(r".*#\s*$", re.MULTILINE)


# matches = hashtag_line_re.findall(text)
def count_hashtag_lines(text):
    # Compile a regular expression to find lines ending with a hashtag
    hashtag_line_re = re.compile(r".*#\s*$", re.MULTILINE)

    # Use finditer to avoid storing all matches at once
    matches = hashtag_line_re.finditer(text)

    # Return the number of matching lines by summing over the iterator
    return sum(1 for _ in matches)


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
    if num_words < 20 or num_words > 100000:
        # why
        # print("num words not in range")
        return False
    if len(s) / num_words < 3 or len(s) / num_words > 10:
        # print("mean word length not in range")
        return False
    if s.count("#") / num_words > 0.1:
        # print("too many hashtags")
        return False
    if s.count("...") / num_words > 0.1:
        # print("too many ellipses")
        return False
    if s.count("•") / num_words > 0.9:
        # print("too many bullet points")
        return False
    if s.count("...") / num_words > 0.3:
        # print("too many ellipses")
        return False
    if sum([1 for w in s.split() if not w.isalpha()]) / len(s.split()) > 0.8:
        # print("too many non-alphabetic characters")
        return False
    # if (
    #     sum(
    #         [
    #             1
    #             for w in s.split()
    #             if w in ["the", "be", "to", "of", "and", "that", "have", "with"]
    #         ]
    #     )
    #     < 2
    # ):
    #     print("not enough stop words")
    #     return False
    return True
