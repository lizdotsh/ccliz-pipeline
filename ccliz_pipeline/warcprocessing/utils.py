import re
import unicodedata

import trafilatura as tf


def process_html(str: str):
    return tf.extract(str)
