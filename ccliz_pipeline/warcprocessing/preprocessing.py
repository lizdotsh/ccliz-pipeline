import trafilatura as tf
from resiliparse.parse.encoding import bytes_to_str


def preprocess_raw_bytes(body: bytes) -> str:
    return tf.html2txt(
        body,
        # bytes_to_str(body),
        # include_comments=False,
        # include_tables=False,
        # favor_precision=True,
        #  no_fallback=True,
    )


#   )
