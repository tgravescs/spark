# This script uses nvidia-smi command so only works with nvidia GPUs. This is a very basic script that lists the index's of the available GPU's where the executor was allocated.
nvidia-smi --query-gpu=index --format=csv,noheader | sed 'N;s/\n/,/'
