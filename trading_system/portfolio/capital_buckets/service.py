from core.models.domain import CapitalBucket, CapitalBucketType


class CapitalBucketService:
    def __init__(self, buckets: list[CapitalBucket]) -> None:
        self.buckets = {b.bucket_type: b for b in buckets}

    def tradable_buckets(self) -> list[CapitalBucketType]:
        return [k for k, b in self.buckets.items() if not b.locked and k != CapitalBucketType.LOCKED_RESERVE]
