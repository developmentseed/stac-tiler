"""stac_tiler.reader."""

import functools
import json
import multiprocessing
import os
import re
from concurrent import futures
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple, Union
from urllib.parse import urlparse

import morecantile
import numexpr
import numpy
import requests

from rio_tiler.errors import InvalidBandName
from rio_tiler_crs import COGReader

from .utils import s3_get_object

TMS = morecantile.tms.get("WebMercatorQuad")
MAX_THREADS = int(os.environ.get("MAX_THREADS", multiprocessing.cpu_count() * 5))
DEFAULT_VALID_TYPE = {
    "image/tiff; application=geotiff",
    "image/tiff; application=geotiff; profile=cloud-optimized",
    "image/vnd.stac.geotiff; cloud-optimized=true",
    "image/tiff",
    "image/x.geotiff",
    "image/jp2",
    "application/x-hdf5",
    "application/x-hdf",
}


def _apply_expression(
    blocks: Sequence[str], bands: Sequence[str], data: numpy.ndarray
) -> numpy.ndarray:
    """Apply rio-tiler expression."""
    data = dict(zip(bands, data))
    return numpy.array(
        [
            numpy.nan_to_num(numexpr.evaluate(bloc.strip(), local_dict=data))
            for bloc in blocks
        ]
    )


@functools.lru_cache(maxsize=512)
def fetch(filepath: str) -> Dict:
    """Fetch items."""
    parsed = urlparse(filepath)
    if parsed.scheme == "s3":
        bucket = parsed.netloc
        key = parsed.path.strip("/")
        return json.loads(s3_get_object(bucket, key))

    elif parsed.scheme in ["https", "http", "ftp"]:
        return requests.get(filepath).json()

    else:
        with open(filepath, "r") as f:
            return json.load(f)


def _get_assets(
    item: Dict,
    include: Optional[Set[str]] = None,
    exclude: Optional[Set[str]] = None,
    include_asset_types: Optional[Set[str]] = None,
    exclude_asset_types: Optional[Set[str]] = None,
) -> Iterator:
    """Get Asset list."""
    for asset, asset_info in item["assets"].items():
        _type = asset_info["type"]

        if exclude and asset in exclude:
            continue

        if (exclude_asset_types and _type in exclude_asset_types) or (
            include and asset not in include
        ):
            continue

        if (include_asset_types and _type not in include_asset_types) or (
            include and asset not in include
        ):
            continue

        yield asset


@dataclass
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
        Get Assets raster info.
    metadata(assets="B01", pmin=5, pmax=95)
        info + stats

    """

    filepath: str
    item: Optional[Dict] = None
    tms: morecantile.TileMatrixSet = TMS
    minzoom: int = TMS.minzoom
    maxzoom: int = TMS.maxzoom
    include_assets: Optional[Set[str]] = None
    exclude_assets: Optional[Set[str]] = None
    include_asset_types: Set[str] = field(default_factory=lambda: DEFAULT_VALID_TYPE)
    exclude_asset_types: Optional[Set[str]] = None

    def __enter__(self):
        """Support using with Context Managers."""
        self.item = self.item or fetch(self.filepath)

        # Get Zooms from proj: ?
        self.bounds: Tuple[float, float, float, float] = self.item["bbox"]

        self.assets = list(
            _get_assets(
                self.item,
                include=self.include_assets,
                exclude=self.exclude_assets,
                include_asset_types=self.include_asset_types,
                exclude_asset_types=self.exclude_asset_types,
            )
        )

        return self

    def __exit__(self, *args):
        """Support using with Context Managers."""
        pass

    def _get_href(self, assets: Sequence[str]) -> Sequence[str]:
        """Validate asset names and return asset's url."""
        for asset in assets:
            if asset not in self.assets:
                raise InvalidBandName(f"{asset} is not a valid asset name.")

        return [self.item["assets"][asset]["href"] for asset in assets]

    def _parse_expression(self, expression: str) -> Sequence[str]:
        """Parse rio-tiler band math expression."""
        _re = re.compile("|".join(sorted(self.assets, reverse=True)))
        assets = list(set(re.findall(_re, expression)))
        return assets

    @property
    def center(self) -> Tuple[float, float, int]:
        """Return COG center + minzoom."""
        return (
            (self.bounds[0] + self.bounds[2]) / 2,
            (self.bounds[1] + self.bounds[3]) / 2,
            self.minzoom,
        )

    def _tile(
        self, assets: Sequence[str], *args: Any, **kwargs: Any
    ) -> Tuple[numpy.ndarray, numpy.ndarray]:
        """Assemble multiple rio_tiler.reader.tile."""

        def worker(asset: str):
            with COGReader(asset, tms=self.tms) as cog:
                return cog.tile(*args, **kwargs)

        with futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            data, masks = zip(*list(executor.map(worker, assets)))
            data = numpy.concatenate(data)
            mask = numpy.all(masks, axis=0).astype(numpy.uint8) * 255
            return data, mask

    def tile(
        self,
        tile_x: int,
        tile_y: int,
        tile_z: int,
        tilesize: int = 256,
        assets: Union[Sequence[str], str] = None,
        expression: Optional[str] = "",  # Expression based on asset names
        asset_expression: Optional[
            str
        ] = "",  # Expression for each asset based on index names
        **kwargs: Any,
    ) -> Tuple[numpy.ndarray, numpy.ndarray]:
        """Read a TMS map tile from COGs."""
        if isinstance(assets, str):
            assets = (assets,)

        if expression:
            assets = self._parse_expression(expression)

        if not assets:
            raise Exception(
                "assets must be passed either via expression or assets options."
            )

        asset_urls = self._get_href(assets)
        data, mask = self._tile(
            asset_urls, tile_x, tile_y, tile_z, expression=asset_expression, **kwargs,
        )

        if expression:
            blocks = expression.split(",")
            data = _apply_expression(blocks, assets, data)

        return data, mask

    def _part(
        self, assets: Sequence[str], *args: Any, **kwargs: Any
    ) -> Tuple[numpy.ndarray, numpy.ndarray]:
        """Assemble multiple COGReader.part."""

        def worker(asset: str):
            with COGReader(asset) as cog:
                return cog.part(*args, **kwargs)

        with futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            data, masks = zip(*list(executor.map(worker, assets)))
            data = numpy.concatenate(data)
            mask = numpy.all(masks, axis=0).astype(numpy.uint8) * 255
            return data, mask

    def part(
        self,
        bbox: Tuple[float, float, float, float],
        max_size: int = 1024,
        assets: Union[Sequence[str], str] = None,
        expression: Optional[str] = "",  # Expression based on asset names
        asset_expression: Optional[
            str
        ] = "",  # Expression for each asset based on index names
        **kwargs: Any,
    ) -> Tuple[numpy.ndarray, numpy.ndarray]:
        """Read part of COGs."""
        if isinstance(assets, str):
            assets = (assets,)

        if expression:
            assets = self._parse_expression(expression)

        if not assets:
            raise Exception(
                "assets must be passed either via expression or assets options."
            )

        asset_urls = self._get_href(assets)
        data, mask = self._part(
            asset_urls, bbox, max_size=max_size, expression=asset_expression, **kwargs
        )

        if expression:
            blocks = expression.split(",")
            data = _apply_expression(blocks, assets, data)

        return data, mask

    def _preview(
        self, assets: Sequence[str], *args: Any, **kwargs: Any
    ) -> Tuple[numpy.ndarray, numpy.ndarray]:
        """Assemble multiple COGReader.preview."""

        def worker(asset: str):
            with COGReader(asset) as cog:
                return cog.preview(*args, **kwargs)

        with futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            data, masks = zip(*list(executor.map(worker, assets)))
            data = numpy.concatenate(data)
            mask = numpy.all(masks, axis=0).astype(numpy.uint8) * 255
            return data, mask

    def preview(
        self,
        assets: Union[Sequence[str], str] = None,
        expression: Optional[str] = "",  # Expression based on asset names
        asset_expression: Optional[
            str
        ] = "",  # Expression for each asset based on index names
        **kwargs: Any,
    ) -> Tuple[numpy.ndarray, numpy.ndarray]:
        """Return a preview of COGs."""
        if isinstance(assets, str):
            assets = (assets,)

        if expression:
            assets = self._parse_expression(expression)

        if not assets:
            raise Exception(
                "assets must be passed either via expression or assets options."
            )

        asset_urls = self._get_href(assets)
        data, mask = self._preview(asset_urls, expression=asset_expression, **kwargs)

        if expression:
            blocks = expression.split(",")
            data = _apply_expression(blocks, assets, data)

        return data, mask

    def _point(self, assets: Sequence[str], *args: Any, **kwargs: Any) -> List:
        """Assemble multiple COGReader.point."""

        def worker(asset: str) -> List:
            with COGReader(asset) as cog:
                return cog.point(*args, **kwargs)

        with futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            return list(executor.map(worker, assets))

    def point(
        self,
        lon: float,
        lat: float,
        assets: Union[Sequence[str], str] = None,
        expression: Optional[str] = "",  # Expression based on asset names
        asset_expression: Optional[
            str
        ] = "",  # Expression for each asset based on index names
        **kwargs: Any,
    ) -> List:
        """Read a value from COGs."""
        if isinstance(assets, str):
            assets = (assets,)

        if expression:
            assets = self._parse_expression(expression)

        if not assets:
            raise Exception(
                "assets must be passed either via expression or assets options."
            )

        asset_urls = self._get_href(assets)
        point = self._point(asset_urls, lon, lat, expression=asset_expression, **kwargs)

        if expression:
            blocks = expression.split(",")
            point = _apply_expression(blocks, assets, point).tolist()

        return point

    def _stats(self, assets: Sequence[str], *args: Any, **kwargs: Any) -> List:
        """Assemble multiple COGReader.stats."""

        def worker(asset: str) -> Dict:
            with COGReader(asset) as cog:
                return cog.stats(*args, **kwargs)

        with futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            return list(executor.map(worker, assets))

    def stats(
        self,
        assets: Union[Sequence[str], str],
        pmin: float = 2.0,
        pmax: float = 98.0,
        **kwargs: Any,
    ) -> Dict:
        """Return array statistics from COGs."""
        if isinstance(assets, str):
            assets = (assets,)

        asset_urls = self._get_href(assets)

        stats = self._stats(asset_urls, pmin, pmax, **kwargs)
        return {asset: stats[ix] for ix, asset in enumerate(assets)}

    def _info(self, assets: Sequence[str]) -> List:
        """Assemble multiple COGReader.stats."""

        def worker(asset: str) -> Dict:
            with COGReader(asset, tms=self.tms) as cog:
                return cog.info

        with futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            return list(executor.map(worker, assets))

    def info(self, assets: Union[Sequence[str], str]) -> Dict:
        """Return info from COGs."""
        if isinstance(assets, str):
            assets = (assets,)

        asset_urls = self._get_href(assets)

        infos = self._info(asset_urls)
        return {asset: infos[ix] for ix, asset in enumerate(assets)}

    def _metadata(self, assets: Sequence[str], *args: Any, **kwargs: Any) -> List:
        """Assemble multiple COGReader.stats."""

        def worker(asset: str) -> Dict:
            with COGReader(asset) as cog:
                return cog.metadata(*args, **kwargs)

        with futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            return list(executor.map(worker, assets))

    def metadata(
        self,
        assets: Union[Sequence[str], str],
        pmin: float = 2.0,
        pmax: float = 98.0,
        **kwargs: Any,
    ) -> Dict:
        """Return array statistics from COGs."""
        if isinstance(assets, str):
            assets = (assets,)

        asset_urls = self._get_href(assets)

        stats = self._metadata(asset_urls, pmin, pmax, **kwargs)
        return {asset: stats[ix] for ix, asset in enumerate(assets)}
