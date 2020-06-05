"""Tests for stac_reader."""

import json
import os
from unittest.mock import patch

import morecantile
import pytest
import rasterio
from rasterio.warp import transform_bounds
from stac_tiler import STACReader

from rio_tiler import constants
from rio_tiler.errors import InvalidBandName
from rio_tiler_crs import COGReader

prefix = os.path.join(os.path.dirname(__file__), "fixtures")
STAC_PATH = os.path.join(prefix, "item.json")
ALL_ASSETS = [
    "thumbnail",
    "overview",
    "info",
    "metadata",
    "visual",
    "B01",
    "B02",
    "B03",
    "B04",
    "B05",
    "B06",
    "B07",
    "B08",
    "B8A",
    "B09",
    "B11",
    "B12",
    "AOT",
    "WVP",
    "SCL",
]


class mock_COGReader(COGReader):
    """Mock COGReader."""

    def __enter__(self):
        """Support using with Context Managers."""
        self.filepath
        assert self.filepath.startswith("https://somewhereovertherainbow.io/")
        cog_path = os.path.join(prefix, os.path.basename(self.filepath))
        self.dataset = rasterio.open(cog_path)
        self.bounds = transform_bounds(
            self.dataset.crs, constants.WGS84_CRS, *self.dataset.bounds, densify_pts=21
        )
        return self


@patch("stac_tiler.reader.s3_get_object")
@patch("stac_tiler.reader.requests")
def test_fetch_stac(requests, s3_get):
    with STACReader(STAC_PATH, include_asset_types=None) as stac:
        assert stac.minzoom == 0
        assert stac.maxzoom == 24
        assert stac.tms.identifier == "WebMercatorQuad"
        assert stac.filepath == STAC_PATH
        assert stac.assets == ALL_ASSETS
    requests.assert_not_called()
    s3_get.assert_not_called()

    with STACReader(STAC_PATH) as stac:
        assert stac.minzoom == 0
        assert stac.maxzoom == 24
        assert stac.tms.identifier == "WebMercatorQuad"
        assert stac.filepath == STAC_PATH
        assert "metadata" not in stac.assets
        assert "thumbnail" not in stac.assets
        assert "info" not in stac.assets
    requests.assert_not_called()
    s3_get.assert_not_called()

    with STACReader(STAC_PATH, include_assets={"B01", "B02"}) as stac:
        assert stac.assets == ["B01", "B02"]
    requests.assert_not_called()
    s3_get.assert_not_called()

    with STACReader(STAC_PATH, include_assets={"B01", "B02"}) as stac:
        assert stac.assets == ["B01", "B02"]
    requests.assert_not_called()
    s3_get.assert_not_called()

    with STACReader(
        STAC_PATH, exclude_assets={"overview", "visual", "AOT", "WVP", "SCL"}
    ) as stac:
        assert stac.assets == [
            "B01",
            "B02",
            "B03",
            "B04",
            "B05",
            "B06",
            "B07",
            "B08",
            "B8A",
            "B09",
            "B11",
            "B12",
        ]
    requests.assert_not_called()
    s3_get.assert_not_called()

    with STACReader(STAC_PATH, include_asset_types={"application/xml"}) as stac:
        assert stac.assets == ["metadata"]
    requests.assert_not_called()
    s3_get.assert_not_called()

    with STACReader(
        STAC_PATH,
        include_asset_types={"application/xml", "image/png"},
        include_assets={"metadata", "overview"},
    ) as stac:
        assert stac.assets == ["metadata"]
    requests.assert_not_called()
    s3_get.assert_not_called()

    # Should raise an error in future versions
    with STACReader(STAC_PATH, include_assets={"B1"}) as stac:
        assert not stac.assets
    requests.assert_not_called()
    s3_get.assert_not_called()

    # HTTP
    class MockResponse:
        def __init__(self, data):
            self.data = data

        def json(self):
            return json.loads(self.data)

    with open(STAC_PATH, "r") as f:
        requests.get.return_value = MockResponse(f.read())

    with STACReader(
        "http://somewhereovertherainbow.io/mystac.json", include_assets={"B01"}
    ) as stac:
        assert stac.assets == ["B01"]
    requests.get.assert_called_once()
    s3_get.assert_not_called()
    requests.mock_reset()

    # S3
    with open(STAC_PATH, "r") as f:
        s3_get.return_value = f.read()

    with STACReader(
        "s3://somewhereovertherainbow.io/mystac.json", include_assets={"B01"}
    ) as stac:
        assert stac.assets == ["B01"]
    requests.assert_not_called()
    s3_get.assert_called_once()


@patch("stac_tiler.reader.COGReader", mock_COGReader)
def test_reader_tiles():
    """Test STACReader.tile."""
    tile = morecantile.Tile(z=9, x=289, y=207)

    with STACReader(STAC_PATH) as stac:
        with pytest.raises(InvalidBandName):
            stac.tile(*tile, assets="B1")

        with pytest.raises(Exception):
            stac.tile(*tile)

        data, mask = stac.tile(*tile, assets="B01")
    assert data.shape == (1, 256, 256)
    assert mask.shape == (256, 256)

    with STACReader(STAC_PATH) as stac:
        data, mask = stac.tile(*tile, expression="B01/B02")
    assert data.shape == (1, 256, 256)
    assert mask.shape == (256, 256)

    with STACReader(STAC_PATH) as stac:
        data, mask = stac.tile(*tile, assets=["B01", "B02"])
    assert data.shape == (2, 256, 256)
    assert mask.shape == (256, 256)

    # This is possible but user should not to it ;-)
    # We are reading B01 and B02 and telling rasterio to return twice bidx 1.
    with STACReader(STAC_PATH) as stac:
        data, mask = stac.tile(*tile, assets=["B01", "B02"], indexes=(1, 1))
    assert data.shape == (4, 256, 256)
    assert mask.shape == (256, 256)

    # Power User might use expression for each assets
    with STACReader(STAC_PATH) as stac:
        data, mask = stac.tile(*tile, assets=["B01", "B02"], asset_expression="b1/2")
    assert data.shape == (2, 256, 256)
    assert mask.shape == (256, 256)

    with STACReader(STAC_PATH, tms=morecantile.tms.get("WorldCRS84Quad")) as stac:
        data, mask = stac.tile(4, 1, 2, assets="B01")
    assert data.shape == (1, 256, 256)
    assert mask.shape == (256, 256)


@patch("stac_tiler.reader.COGReader", mock_COGReader)
def test_reader_part():
    """Test STACReader.part."""
    bbox = (23.7, 31.506, 24.1, 32.514)

    with STACReader(STAC_PATH) as stac:
        with pytest.raises(InvalidBandName):
            stac.part(bbox, assets="B1")

        with pytest.raises(Exception):
            stac.part(bbox)

        data, mask = stac.part(bbox, assets="B01")
    assert data.shape == (1, 189, 68)
    assert mask.shape == (189, 68)

    with STACReader(STAC_PATH) as stac:
        data, mask = stac.part(bbox, expression="B04/B02")
    assert data.shape == (1, 1024, 371)
    assert mask.shape == (1024, 371)

    with STACReader(STAC_PATH) as stac:
        data, mask = stac.part(bbox, assets=["B04", "B02"])
    assert data.shape == (2, 1024, 371)
    assert mask.shape == (1024, 371)

    # This is possible but user should not to it ;-)
    # We are reading B01 and B02 and telling rasterio to return twice bidx 1.
    with STACReader(STAC_PATH) as stac:
        data, mask = stac.part(bbox, assets=["B04", "B02"], indexes=(1, 1))
    assert data.shape == (4, 1024, 371)
    assert mask.shape == (1024, 371)

    # Power User might use expression for each assets
    with STACReader(STAC_PATH) as stac:
        data, mask = stac.part(bbox, assets=["B04", "B02"], asset_expression="b1/2")
    assert data.shape == (2, 1024, 371)
    assert mask.shape == (1024, 371)

    with STACReader(STAC_PATH) as stac:
        data, mask = stac.part(bbox, assets="B04", max_size=None)
    assert data.shape == (1, 1129, 408)
    assert mask.shape == (1129, 408)


@patch("stac_tiler.reader.COGReader", mock_COGReader)
def test_reader_preview():
    """Test STACReader.preview."""
    with STACReader(STAC_PATH) as stac:
        with pytest.raises(InvalidBandName):
            stac.preview(assets="B1")

        with pytest.raises(Exception):
            stac.preview()

        data, mask = stac.preview(assets="B01")
    assert data.shape == (1, 183, 183)
    assert mask.shape == (183, 183)

    with STACReader(STAC_PATH) as stac:
        data, mask = stac.preview(expression="B04/B02")
    assert data.shape == (1, 1024, 1024)
    assert mask.shape == (1024, 1024)

    with STACReader(STAC_PATH) as stac:
        data, mask = stac.preview(assets=["B04", "B02"])
    assert data.shape == (2, 1024, 1024)
    assert mask.shape == (1024, 1024)

    # This is possible but user should not to it ;-)
    # We are reading B01 and B02 and telling rasterio to return twice bidx 1.
    with STACReader(STAC_PATH) as stac:
        data, mask = stac.preview(assets=["B04", "B02"], indexes=(1, 1))
    assert data.shape == (4, 1024, 1024)
    assert mask.shape == (1024, 1024)

    # Power User might use expression for each assets
    with STACReader(STAC_PATH) as stac:
        data, mask = stac.preview(assets=["B04", "B02"], asset_expression="b1/2")
    assert data.shape == (2, 1024, 1024)
    assert mask.shape == (1024, 1024)

    with STACReader(STAC_PATH) as stac:
        data, mask = stac.preview(assets="B04", max_size=512)
    assert data.shape == (1, 512, 512)
    assert mask.shape == (512, 512)


@patch("stac_tiler.reader.COGReader", mock_COGReader)
def test_reader_point():
    """Test STACReader.point."""
    lat = 32
    lon = 23.7

    with STACReader(STAC_PATH) as stac:
        with pytest.raises(InvalidBandName):
            stac.point(lon, lat, assets="B1")

        with pytest.raises(Exception):
            stac.point(lon, lat)

        data = stac.point(lon, lat, assets="B01")
    assert len(data) == 1

    with STACReader(STAC_PATH) as stac:
        data = stac.point(lon, lat, expression="B04/B02")
    assert len(data) == 1

    with STACReader(STAC_PATH) as stac:
        data = stac.point(lon, lat, assets=["B04", "B02"])
    assert len(data) == 2

    # This is possible but user should not to it ;-)
    # We are reading B01 and B02 and telling rasterio to return twice bidx 1.
    with STACReader(STAC_PATH) as stac:
        data = stac.point(lon, lat, assets=["B04", "B02"], indexes=(1, 1))
    assert len(data) == 2
    assert len(data[0]) == 2

    # Power User might use expression for each assets
    with STACReader(STAC_PATH) as stac:
        data = stac.point(lon, lat, assets=["B04", "B02"], asset_expression="b1/2")
    assert len(data) == 2


@patch("stac_tiler.reader.COGReader", mock_COGReader)
def test_reader_stats():
    """Test STACReader.stats."""
    with STACReader(STAC_PATH) as stac:
        with pytest.raises(InvalidBandName):
            stac.stats(assets="B1")

        data = stac.stats(assets="B01")
    assert len(data.keys()) == 1
    assert data["B01"]

    with STACReader(STAC_PATH) as stac:
        data = stac.stats(assets=["B04", "B02"])
    assert len(data.keys()) == 2
    assert data["B02"]
    assert data["B04"]


@patch("stac_tiler.reader.COGReader", mock_COGReader)
def test_reader_info():
    """Test STACReader.info."""
    with STACReader(STAC_PATH) as stac:
        with pytest.raises(InvalidBandName):
            stac.info(assets="B1")

        data = stac.info(assets="B01")
    assert len(data.keys()) == 1
    assert data["B01"]

    with STACReader(STAC_PATH) as stac:
        data = stac.info(assets=["B04", "B02"])
    assert len(data.keys()) == 2
    assert data["B02"]
    assert data["B04"]


@patch("stac_tiler.reader.COGReader", mock_COGReader)
def test_reader_metadata():
    """Test STACReader.metadata."""
    with STACReader(STAC_PATH) as stac:
        with pytest.raises(InvalidBandName):
            stac.metadata(assets="B1")

        data = stac.metadata(assets="B01")
    assert len(data.keys()) == 1
    assert data["B01"]
    assert data["B01"]["statistics"]

    with STACReader(STAC_PATH) as stac:
        data = stac.metadata(assets=["B04", "B02"])
    assert len(data.keys()) == 2
    assert data["B02"]
    assert data["B04"]
