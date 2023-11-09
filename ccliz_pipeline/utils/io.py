from xopen import xopen


def dump_bytes_to_gz(path: str, data: bytes, **kwargs):
    with xopen(path, "wb", **kwargs) as f:
        f.write(data)
