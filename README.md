# stac-tiler

A rio-tiler plugin to handler STAC items


[![Packaging status](https://badge.fury.io/py/stac-tiler.svg)](https://badge.fury.io/py/stac-tiler)
[![CircleCI](https://circleci.com/gh/developmentseed/stac-tiler.svg?style=svg)](https://circleci.com/gh/cogeotiff/stac-tiler)
[![codecov](https://codecov.io/gh/developmentseed/stac-tiler/branch/master/graph/badge.svg)](https://codecov.io/gh/cogeotiff/stac-tiler)


## Install

```bash
$ pip install pip -U
$ pip install stac-tiler --pre  # stac-tiler is in pre-release 0.0rc1 version 

# Or using source

$ pip install git+http://github.com/developmentseed/stac-tiler
```

## How To

stac-tiler is based on [rio-tiler-crs](https://github.com/cogeotiff/rio-tiler-crs) and [morecantile](https://github.com/developmentseed/morecantile).

## Docs

<details>

```
class STACReader:
    """
    STAC + Cloud Optimized GeoTIFF Reader.

    Examples
    --------
    with STACReader(stac_path) as stac:
        stac.tile(...)

    my_stac = {
        "type": "Feature",
        "stac_version": "1.0.0",
        ...
    }
    with STACReader(None, item=my_stac) as stac:
        stac.tile(...)

    Attributes
    ----------
    filepath: str
        STAC Item path, URL or S3 URL.
    item: Dict, optional
        STAC Item dict.
    tms: morecantile.TileMatrixSet, optional
        TileMatrixSet to use, default is WebMercatorQuad.
    minzoom: int, optional
        Set minzoom for the tiles.
    minzoom: int, optional
        Set maxzoom for the tiles.
    include_assets: Set, optional
        Only accept some assets.
    exclude_assets: Set, optional
        Exclude some assets.
    include_asset_types: Set, optional
        Only include some assets base on their type
    include_asset_types: Set, optional
        Exclude some assets base on their type

    Properties
    ----------
    bounds: tuple[float]
        STAC bounds in WGS84 crs.
    center: tuple[float, float, int]
        STAC item center + minzoom

    Methods
    -------
    tile(0, 0, 0, assets="B01", expression="B01/B02")
        Read a map tile from the COG.
    part((0,10,0,10), assets="B01", expression="B1/B20", max_size=1024)
        Read part of the COG.
    preview(assets="B01", max_size=1024)
        Read preview of the COG.
    point((10, 10), assets="B01")
        Read a point value from the COG.
    stats(assets="B01", pmin=5, pmax=95)
        Get Raster statistics.
    info(assets="B01")
    """
```

</details>


- **STACReader.tile()**: Read map tile from STAC assets

```python
with STACReader("stac.json") as stac:
    tile, mask = stac.tile(1, 2, 3, tilesize=256, assets=["red", "green"])

# With expression
with STACReader("stac.json") as stac:
    tile, mask = cog.tile(1, 2, 3, tilesize=256, expression="red/green")
```

- **STACReader.part()**: Read part of STAC assets

```python
with STACReader("stac.json") as stac:
    data, mask = stac.part((10, 10, 20, 20), assets=["red", "green"])

# Limit output size (default is set to 1024)
with STACReader("stac.json") as stac:
    data, mask = stac.part((10, 10, 20, 20), max_size=2000, assets=["red", "green"])

# Read high resolution
with STACReader("stac.json") as stac:
    data, mask = stac.part((10, 10, 20, 20), max_size=None, assets=["red", "green"])

# With expression
with STACReader("stac.json") as stac:
    data, mask = stac.part((10, 10, 20, 20), expression="red/green")
```

- **STACReader.preview()**: Read a preview of STAC assets

```python
with STACReader("stac.json") as stac:
    data, mask = stac.preview(assets=["red", "green"])

# With expression
with STACReader("stac.json") as stac:
    data, mask = stac.preview(expression="red/green")
```

- **STACReader.point()**: Read point value of STAC assets

```python
with STACReader("stac.json") as stac:
    pts = stac.point(-100, 25, assets=["red", "green"])


# With expression
with STACReader("stac.json") as stac:
    pts = stac.point(-100, 25, expression="red/green")
```

- **STACReader.info()**: Return simple metadata for STAC assets

```python
with STACReader("stac.json") as stac:
    info = stac.info("B01")
{
    "B01": {
        "bounds": [23.10607624352815, 31.50517374437416, 24.296464503939944, 32.51933487169619],
        "center": [23.701270373734047, 32.012254308035175, 8],
        "minzoom": 8,
        "maxzoom": 11,
        "band_metadata": [[1, {}]],
        "band_descriptions": [[1, "band1"]],
        "dtype": "uint16",
        "colorinterp": ["gray"],
        "nodata_type": "Nodata"
    }
}
```

- **STACReader.stats()**: Return statistics for STAC assets (Min/Max/Stdev)

```python
with STACReader("stac.json") as stac:
    print(stac.stats(["B01"]))
{
    "B01": {
        "1": {
            "pc": [
                324,
                5046
            ],
            "min": 133,
            "max": 8582,
            "std": 1230.6977195618235,
            "histogram": [
                [
                    199042, 178438, 188457, 118369, 57544, 20622, 9275, 2885, 761, 146
                ],
                [
                    133, 977.9, 1822.8, 2667.7, 3512.6, 4357.5, 5202.4, 6047.3, 6892.2, 7737.099999999999, 8582
                ]
            ]
        }
    }
}
```

## Contribution & Development

Issues and pull requests are more than welcome.

**dev install**

```bash
$ git clone https://github.com/developmentseed/stac-tiler.git
$ cd stac-tiler
$ pip install -e .[dev]
```

**Python >=3.7 only**

This repo is set to use `pre-commit` to run *isort*, *flake8*, *pydocstring*, *black* ("uncompromising Python code formatter") and mypy when committing new code.

```
$ pre-commit install

$ git add .

$ git commit -m'my change'
isort....................................................................Passed
black....................................................................Passed
Flake8...................................................................Passed
Verifying PEP257 Compliance..............................................Passed
mypy.....................................................................Passed

$ git push origin
```