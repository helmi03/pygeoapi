# =================================================================
#
# Authors: Joana Simoes <helmi03@gmail.com>
#
# Copyright (c) 2023 Joana Simoes
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import json
import logging
import requests
from pathlib import Path
from urllib.parse import urlparse

from pygeoapi.provider.tile import (
    ProviderTileNotFoundError)
from pygeoapi.provider.base import (ProviderConnectionError,
                                    ProviderInvalidQueryError,
                                    ProviderGenericError)
from pygeoapi.provider.mvt_tippecanoe import MVTTippecanoeProvider
from pygeoapi.models.provider.base import (
    TileSetMetadata, TileMatrixSetEnum, LinkType)
from pygeoapi.models.provider.mvt import MVTTilesJson

from pygeoapi.util import is_url, url_join

LOGGER = logging.getLogger(__name__)


class MVTPmtilesProvider(MVTTippecanoeProvider):
    """MVT PMTiles Provider
    Provider for serving tiles served by go-pmtiles
    https://github.com/protomaps/go-pmtiles
    It supports tiles from an url.
    """

    def __init__(self, MVTTippecanoeProvider):
        """
        Initialize object

        :param provider_def: provider definition

        :returns: pygeoapi.provider.MVT.MVTPmtilesProvider
        """

        super().__init__(MVTTippecanoeProvider)

        # Pre-rendered tiles served from a static url
        if is_url(self.data):
            url = urlparse(self.data)
            baseurl = f'{url.scheme}://{url.netloc}'
            layer = f'/{self.get_layer()}'

            LOGGER.debug('Extracting layer name from URL')
            LOGGER.debug(f'Layer: {layer}')

            tilepath = f'{layer}/tiles'
            servicepath = f'{tilepath}/{{tileMatrixSetId}}/{{tileMatrix}}/{{tileRow}}/{{tileCol}}?f=mvt'  # noqa

            self._service_url = url_join(baseurl, servicepath)

            self._service_metadata_url = url_join(
                self.service_url.split('{tileMatrix}/{tileRow}/{tileCol}')[0],
                'metadata')

            metadata_path = f'{baseurl}/{layer}.json'
            head = requests.head(metadata_path)
            if head.status_code != 200:
                msg = f'Service metadata does not exist: {metadata_path}'
                LOGGER.error(msg)
                LOGGER.warning(msg)
            self._service_metadata_url = metadata_path

        # Pre-rendered tiles served from a local path
        else:
            data_path = Path(self.data)
            if not data_path.exists():
                msg = f'Service does not exist: {self.data}'
                LOGGER.error(msg)
                raise ProviderConnectionError(msg)
            self._service_url = data_path
            metadata_path = data_path.joinpath('metadata')
            if not metadata_path.exists():
                msg = f'Service metadata does not exist: {metadata_path.name}'
                LOGGER.error(msg)
                LOGGER.warning(msg)
            self._service_metadata_url = metadata_path

    def __repr__(self):
        return f'<MVTPmtilesProvider> {self.data}'

    def get_html_metadata(self, dataset, server_url, layer, tileset,
                          title, description, keywords, **kwargs):

        service_url = url_join(
            server_url,
            f'collections/{dataset}/tiles/{tileset}/{{tileMatrix}}/{{tileRow}}/{{tileCol}}?f=mvt')  # noqa
        metadata_url = url_join(
            server_url,
            f'collections/{dataset}/tiles/{tileset}/metadata')

        metadata = dict()
        metadata['id'] = dataset
        metadata['title'] = title
        metadata['tileset'] = tileset
        metadata['collections_path'] = service_url
        metadata['json_url'] = f'{metadata_url}?f=json'

        try:
            metadata_json_content = self.get_metadata_from_URL(self.service_metadata_url) # noqa

            # content = MVTTilesJson(**metadata_json_content)
            content = MVTTilesJson()
            if "name" in metadata_json_content:
                content.name = metadata_json_content["name"]
            if "description" in metadata_json_content:
                content.description = metadata_json_content["description"]
            if "attribution" in metadata_json_content:
                content.attribution = metadata_json_content["attribution"]
            if "bounds" in metadata_json_content:
                content.bounds = metadata_json_content["bounds"]
            if "center" in metadata_json_content:
                content.center = metadata_json_content["center"]
            if "minzoom" in metadata_json_content:
                content.minzoom = metadata_json_content["minzoom"]
            if "maxzoom" in metadata_json_content:
                content.maxzoom = metadata_json_content["maxzoom"]
            content.tiles = [service_url]
            content.vector_layers = metadata_json_content["vector_layers"]
            metadata['metadata'] = content.dict()
            # Some providers may not implement tilejson metadata
            metadata['tilejson_url'] = f'{metadata_url}?f=tilejson'
        except ProviderConnectionError:
            # No vendor metadata JSON
            pass

        return metadata

    def get_vendor_metadata(self, dataset, server_url, layer, tileset,
                            title, description, keywords, **kwargs):
        """
        Gets tile metadata in tilejson format
        """

        try:
            metadata_json_content = self.get_metadata_from_URL(self.service_metadata_url) # noqa

            service_url = url_join(
                server_url,
                f'collections/{dataset}/tiles/{tileset}/{{z}}/{{x}}/{{y}}?f=mvt')  # noqa

            content = MVTTilesJson()
            if "name" in metadata_json_content:
                content.name = metadata_json_content["name"]
            if "description" in metadata_json_content:
                content.description = metadata_json_content["description"]
            if "attribution" in metadata_json_content:
                content.attribution = metadata_json_content["attribution"]
            if "bounds" in metadata_json_content:
                content.bounds = metadata_json_content["bounds"]
            if "center" in metadata_json_content:
                content.center = metadata_json_content["center"]
            if "minzoom" in metadata_json_content:
                content.minzoom = metadata_json_content["minzoom"]
            if "maxzoom" in metadata_json_content:
                content.maxzoom = metadata_json_content["maxzoom"]
            content.tiles = [service_url]
            content.vector_layers = metadata_json_content["vector_layers"]
            return content.dict()
        except ProviderConnectionError:
            msg = f'No tiles metadata json available: {self.service_metadata_url}'  # noqa
            LOGGER.error(msg)
            raise ProviderConnectionError(msg)
