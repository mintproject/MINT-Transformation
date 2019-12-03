import os

import fiona, sys, slugify
from tqdm.auto import tqdm

infile = sys.argv[1]
output_dir = sys.argv[2]
area_name = sys.argv[3]

with fiona.open(infile, "r") as f:
    # don't know how to set the crs correct for fiona...
    assert f.crs == {'init': 'epsg:4326'}
    crs = {'no_defs': True, 'ellps': 'WGS84', 'datum': 'WGS84', 'proj': 'longlat'}

    for line in tqdm(iter(f)):
        name = line['properties'][area_name]
        name = slugify.slugify(name)

        outfile = os.path.join(output_dir, f"{name}.shp")
        if os.path.exists(outfile):
            with fiona.open(outfile, "a") as g:
                g.write(line)
        else:
            with fiona.open(outfile, "w", driver=f.driver, crs=crs, schema=f.schema) as g:
                g.write(line)
