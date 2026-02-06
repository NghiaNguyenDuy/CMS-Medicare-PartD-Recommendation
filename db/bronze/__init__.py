# Make bronze module importable
from .05_ingest_geography import ingest_zipcode_geo

__all__ = ['ingest_zipcode_geo']
