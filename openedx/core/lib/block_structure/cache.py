"""
Module for the Cache class for BlockStructure objects.
"""
# pylint: disable=protected-access
from logging import getLogger

from openedx.core.djangoapps.content.block_structure import models, config
from openedx.core.lib.cache_utils import zpickle, zunpickle

from .block_structure import BlockStructureBlockData
from .exceptions import BlockStructureNotFound
from .factory import BlockStructureFactory


logger = getLogger(__name__)  # pylint: disable=C0103


class BlockStructureCache(object):
    """
    Cache for BlockStructure objects.
    """
    def __init__(self, cache):
        """
        Arguments:
            cache (django.core.cache.backends.base.BaseCache) - The
                cache into which cacheable data of the block structure
                is to be serialized.
        """
        self._cache = cache

    def add(self, block_structure):
        """
        Store a compressed and pickled serialization of the given
        block structure into the given cache.

        The key in the cache is 'root.key.<root_block_usage_key>'.
        The data stored in the cache includes the structure's
        block relations, transformer data, and block data.

        Arguments:
            block_structure (BlockStructure) - The block structure
                that is to be serialized to the given cache.
        """
        serialized_data = self._serialize(block_structure)

        bs_model = self._create_or_update_model(block_structure, serialized_data)
        self._add_to_cache(block_structure, serialized_data, bs_model)

    def get(self, root_block_usage_key):
        """
        Deserializes and returns the block structure starting at
        root_block_usage_key from the given cache, if it's found in the cache.

        The given root_block_usage_key must equate the root_block_usage_key
        previously passed to serialize_to_cache.

        Arguments:
            root_block_usage_key (UsageKey) - The usage_key for the root
                of the block structure that is to be deserialized from
                the given cache.

        Returns:
            BlockStructure - The deserialized block structure starting
            at root_block_usage_key, if found in the cache.

        Raises:
            BlockStructureNotFound if the root_block_usage_key is not
            found in the cache.
        """
        bs_model = self._get_current_model(root_block_usage_key)

        try:
            serialized_data = self._get_from_cache(root_block_usage_key, bs_model)
        except BlockStructureNotFound:
            serialized_data = self._get_from_store(root_block_usage_key, bs_model)

        return self._deserialize(serialized_data, root_block_usage_key)

    def delete(self, root_block_usage_key):
        """
        Deletes the block structure for the given root_block_usage_key
        from the given cache.

        Arguments:
            root_block_usage_key (UsageKey) - The usage_key for the root
                of the block structure that is to be removed from
                the cache.
        """
        self._cache.delete(self._encode_root_cache_key(root_block_usage_key))
        logger.info(
            "Deleted BlockStructure %r from the cache.",
            root_block_usage_key,
        )

    def _get_model(self, root_block_usage_key):
        if config.is_enabled(config.STORAGE_BACKING_FOR_CACHE):
            return models.BlockStructure.get_current(root_block_usage_key)
        else:
            return None

    def _create_or_update_model(self, block_structure, serialized_data):
        if config.is_enabled(config.STORAGE_BACKING_FOR_CACHE):
            return models.BlockStructure.update_or_create_with_data(
                serialized_data,
                data_usage_key=block_structure,
                data_version=block_structure,
                data_edit_timestamp=block_structure,
                transformers_schema_version=block_structure,
                block_structure_schema_version=block_structure,
            )
        else:
            return None

    def _add_to_cache(self, block_structure, serialized_data, bs_model):
        cache_key = self._encode_root_cache_key(block_structure.root_block_usage_key, bs_model)

        # Set the timeout value for the cache to 1 day as a fail-safe
        # in case the signal to invalidate the cache doesn't come through.
        timeout_in_seconds = 60 * 60 * 24
        self._cache.set(
            cache_key,
            serialized_data,
            timeout=timeout_in_seconds,
        )

        logger.info(
            "Wrote BlockStructure %s to cache, size: %s",
            block_structure.root_block_usage_key,
            len(serialized_data),
        )

    def _get_from_cache(self, root_block_usage_key, bs_model):
        cache_key = self._encode_root_cache_key(root_block_usage_key, bs_model)

        serialized_data = self._cache.get(cache_key)
        if not serialized_data:
            logger.info(
                "Did not find BlockStructure %r in the cache.",
                root_block_usage_key,
            )
            raise BlockStructureNotFound(root_block_usage_key)
        else:
            logger.info(
                "Read BlockStructure %r from cache, size: %s",
                root_block_usage_key,
                len(serialized_data),
            )
        return serialized_data

    def _get_from_store(self, root_block_usage_key, bs_model):
        if not config.is_enabled(config.STORAGE_BACKING_FOR_CACHE):
            raise BlockStructureNotFound(root_block_usage_key)

        serialized_data = bs_model.get_serialized_data()

        # TODO update log statements with versioning info
        logger.info(
            "Read BlockStructure %r from storage, size: %s",
            root_block_usage_key,
            len(serialized_data),
        )
        return serialized_data

    def _serialize(self, block_structure):
        data_to_cache = (
            block_structure._block_relations,
            block_structure.transformer_data,
            block_structure._block_data_map,
        )
        return zpickle(data_to_cache)

    def _deserialize(self, serialized_data, root_block_usage_key):
        block_relations, transformer_data, block_data_map = zunpickle(serialized_data)
        return BlockStructureFactory.create_new(
            root_block_usage_key,
            block_relations,
            transformer_data,
            block_data_map,
        )

    @classmethod
    def _encode_root_cache_key(cls, root_block_usage_key, bs_model):
        """
        Returns the cache key to use for storing the block structure
        for the given root_block_usage_key.
        """
        if config.is_enabled(config.STORAGE_BACKING_FOR_CACHE):
            assert bs_model is not None
            return u':'.join(
                field_name +
                '@' +
                unicode(getattr(bs_model, field_name))
                for field_name in
                [
                    u'data_usage_key',
                    u'data_version',
                    u'data_edit_timestamp',
                    u'transformers_schema_version',
                    u'block_structure_schema_version',
                ]
            )

        else:
            return "v{version}.root.key.{root_usage_key}".format(
                version=unicode(BlockStructureBlockData.VERSION),
                root_usage_key=unicode(root_block_usage_key),
            )
