from ipykernel.kernelapp import IPKernelApp
from .dunky_kernel import DunkyKernel

IPKernelApp.launch_instance(kernel_class=DunkyKernel)
