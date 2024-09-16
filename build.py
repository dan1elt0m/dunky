import json
import os
from tempfile import TemporaryDirectory
from jupyter_client.kernelspec import KernelSpecManager
from hatchling.builders.hooks.plugin.interface import BuildHookInterface

kernel_json = {
    "argv": [
        "python",
        "-m",
        "dunky_kernel",
        "-f",
        "{connection_file}"
    ],
    "display_name": "Dunky",
    "language": "sql",
    "codemirror_mode": "sql"
}


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        self.install_kernelspec()

    def install_kernelspec(self):
        kernel_spec = KernelSpecManager()
        with TemporaryDirectory() as td:
            os.chmod(td, 0o755)
            with open(os.path.join(td, 'kernel.json'), 'w') as f:
                json.dump(kernel_json, f, sort_keys=True)
            kernel_spec.install_kernel_spec(td, 'postgres', user=True)
