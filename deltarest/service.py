from flask import Flask


class DeltaRESTService(Flask):
    def __init__(self,
                 delta_storage_root: str,
                 batch_interval_in_sec: float = 10,
                 payload_limit_in_bytes: float = float('inf')):
        super.__init__(Flask)
