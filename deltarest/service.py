from overrides import overrides

from flask import Flask, request, Response

from .rest import DeltaRESTAdapter

class DeltaRESTService(Flask):
    def __init__(self,
                 delta_storage_root: str,
                 batch_interval_in_sec: float = 10,
                 payload_limit_in_bytes: float = float('inf')):
        super.__init__(Flask)
        self.rest_adapter = DeltaRESTAdapter(delta_storage_root)

    @overrides
    def run(self, host=None, port=None, debug=None, load_dotenv=True,
            **options):
        @self.route('/tables', methods=['GET'])
        def handle_get():
            return self.rest_adapter.get(request.full_path)

