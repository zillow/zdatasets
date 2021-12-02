from typing import Callable

TXF_REGISTERED_DATASETS_ATTRIBUTE = "_txf_registered_datasets"
TXF_CALLBACKS_ATTRIBUTE = "_txf_callbacks"
TXF_METADATA_ATTRIBUTE = "_txf_metadata_accumulator"


def add_txf_attributes(flow):
    if not hasattr(flow, TXF_REGISTERED_DATASETS_ATTRIBUTE):
        setattr(flow, TXF_REGISTERED_DATASETS_ATTRIBUTE, dict())
    if not hasattr(flow, TXF_CALLBACKS_ATTRIBUTE):
        setattr(flow, TXF_CALLBACKS_ATTRIBUTE, {})
    if not hasattr(flow, TXF_METADATA_ATTRIBUTE):
        setattr(flow, TXF_METADATA_ATTRIBUTE, {})


def add_callback(flow, caller: str, func: Callable):
    add_txf_attributes(flow)
    flow._txf_callbacks[caller] = func
