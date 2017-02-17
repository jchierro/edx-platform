"""
Models used by the block structure framework.
"""

from django.db import models
from openedx.core.djangoapps.xmodule_django.models import UsageKeyField
from openedx.core.lib.block_structure.exceptions import BlockStructureNotFound


class BlockStructure(models.Model):
    """
    Model for storing Block Structure information.
    """
    data_usage_key = UsageKeyField(
        u'Identifier of the data being collected.',
        blank=False,
        max_length=255,
        db_index=True,
    )
    data_version = models.CharField(
        u'Version of the data at the time of collection.',
        blank=True,
        max_length=255,
    )
    data_edit_timestamp = models.DateTimeField(
        u'Edit timestamp of the data at the time of collection.',
        blank=True,
        null=True,
    )

    transformers_schema_version = models.CharField(
        u'Representation of the schema version of the transformers used during collection.',
        blank=False,
        max_length=255,
    )
    block_structure_schema_version = models.CharField(
        u'Version of the block structure schema at the time of collection.',
        blank=False,
        max_length=255,
    )

    data = models.FileField()

    def get_serialized_data(self):
        """
        Returns the collected data for this instance.
        """
        return self.data.read()

    @classmethod
    def get_current(cls, data_usage_key):
        """
        Returns the entry associated with the given data_usage_key.
        Raises:
             BlockStructureNotFound if an entry for data_usage_key is not found.
        """
        try:
            return cls.objects.get(data_usage_key=data_usage_key)
        except cls.DoesNotExist:
            raise BlockStructureNotFound(data_usage_key)

    @classmethod
    def update_or_create_with_data(cls, serialized_data, **kwargs):
        """
        """
        cls.objects.update_or_create(**kwargs)


    def delete(self):
        # TODO override to delete underyling file object also
        pass
