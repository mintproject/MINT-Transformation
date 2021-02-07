import glob
import os

from funcs.gdal.raster import Raster, BoundingBox

try:
    from topoflow.utils import regrid
    from topoflow.utils import import_grid
except ModuleNotFoundError:
    from .topoflow.utils import regrid
    from .topoflow.utils import import_grid

from tqdm.auto import tqdm

from multiprocessing import Pool


def crop_geotiff(args):
    in_file, out_crop_file, out_bounds, out_xres_sec, out_yres_sec = args
    regrid.regrid_geotiff(in_file=in_file, out_file=out_crop_file, out_bounds=out_bounds,
                      out_xres_sec=out_xres_sec, out_yres_sec=out_yres_sec,
                      RESAMPLE_ALGO='bilinear', REPORT=False)
    return True


def create_rts_rti(tif_files, out_file, crop_dir: str, out_bounds: BoundingBox, out_xres_sec: int, out_yres_sec: int, unit_multiplier: float, skip_crop_on_exist: bool):
    """Create RTS file from TIF files. Names of TIF files must be sorted by time"""
    assert out_file.endswith(".rts") and len(out_file.split(".rts")) == 2
    assert len(tif_files) > 0

    pool = Pool()
    # crop the data first
    tif_files = sorted(tif_files)
    out_bounds = [out_bounds.x_min, out_bounds.y_min, out_bounds.x_max, out_bounds.y_max]
    out_crop_files = [os.path.join(crop_dir, os.path.basename(tif_file)) for tif_file in tif_files]

    # res = list(pool.imap_unordered(crop_geotiff, [
    #     (tif_file, out_crop_file, out_bounds, out_xres_sec, out_yres_sec)
    #     for tif_file, out_crop_file in zip(tif_files, out_crop_files)
    # ]))
    # print(res)
    for tif_file, out_crop_file in tqdm(zip(tif_files, out_crop_files)):
        if skip_crop_on_exist and os.path.exists(out_crop_file):
            continue
        args = (tif_file, out_crop_file, out_bounds, out_xres_sec, out_yres_sec)
        assert crop_geotiff(args)

    # load the crop file and write rts and rti
    with open(out_file, "wb") as f:
        for out_crop_file in out_crop_files:
            assert os.path.exists(out_crop_file)
            raster = Raster.from_geotiff(out_crop_file)
            (raster.data * unit_multiplier).tofile(f)

        raster = Raster.from_geotiff(out_crop_files[0])
        raster.data = raster.data *  unit_multiplier
        tmp_file = out_file.replace(".rts", ".tif")
        raster.to_geotiff(tmp_file)

        rti_outfile = out_file.replace(".rts", ".rti")
        create_rti(tmp_file, rti_outfile)
        os.remove(tmp_file)


def create_rti(tif_file, out_file):
    """Create RTI from the TIF file"""
    import_grid.read_from_geotiff(tif_file, REPORT=False, rti_file=out_file)


if __name__ == '__main__':
    create_rts_rti(
        glob.glob("/home/rook/workspace/mint/MINT-Transformation/examples/topoflow4/dev/data/geotiff/*.tif"),
        "/home/rook/workspace/mint/MINT-Transformation/examples/topoflow4/dev/data/output.rts",
        "/home/rook/workspace/mint/MINT-Transformation/examples/topoflow4/dev/data/geotiff_crop",
        BoundingBox(34.221249999999, 7.362083333332, 36.450416666666, 9.503749999999),
        60, 60, 1)
