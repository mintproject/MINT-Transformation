import os, numpy as np
from pathlib import Path

if __name__ == "__main__":
    dir = "/Users/rook/workspace/MINT/MINT-Transformation/data/alwero_gldas_2008_30"
    dir = "/Users/rook/workspace/MINT/MINT-Transformation/data/mint/topoflow/alwero/gldas/2008_30"
    dir = "/Users/rook/workspace/MINT/MINT-Transformation/data/mint/topoflow/alwero/gldas/2008_30/cropped_region"

    print("Check file in", dir)
    for file in Path(dir).iterdir():
        if any(file.name.endswith(ext) for ext in [".rts", ".npz"]):
            print(">>> process", file.name)
            array = np.load(str(file))['grid']
            print(np.max(array), np.min(array), array.shape)