from dataclasses import dataclass
from datetime import datetime


@dataclass
class TemporalCoverage:
    start_time: datetime
    end_time: datetime


@dataclass
class SpatialCoverage:
    xmin: float
    ymin: float
    xmax: float
    ymax: float


@dataclass
class Metadata:
    temporal_coverage: TemporalCoverage
    spatial_coverage: SpatialCoverage

    def __init__(self, start_time, end_time, xmin, ymin, xmax, ymax):
        self.temporal_coverage = TemporalCoverage(start_time, end_time)
        self.spatial_coverage = SpatialCoverage(xmin, ymin, xmax, ymax)
