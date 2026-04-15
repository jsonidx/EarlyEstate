from app.adapters.insolvency import InsolvencyAdapter
from app.adapters.lbs import LBSAdapter
from app.adapters.sparkasse import SparkasseAdapter
from app.adapters.zvg import ZVGAdapter

ADAPTER_REGISTRY: dict[str, type] = {
    InsolvencyAdapter.source_key: InsolvencyAdapter,
    SparkasseAdapter.source_key: SparkasseAdapter,
    LBSAdapter.source_key: LBSAdapter,
    ZVGAdapter.source_key: ZVGAdapter,
}

__all__ = ["InsolvencyAdapter", "LBSAdapter", "SparkasseAdapter", "ZVGAdapter", "ADAPTER_REGISTRY"]
