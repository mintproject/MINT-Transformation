import sys
import numpy as np

if __name__ == "__main__":
    args = sys.argv[1:]
    input_path = args[0]
    output_path = args[1]

    x = np.fromfile(input_path)
    x = x / 3600

    x.tofile(output_path)

