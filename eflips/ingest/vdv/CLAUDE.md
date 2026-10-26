# VDV ingester

- `_xmldata.py` began as xsdata output but has been hand-modified; never regenerate it.
- The VDV 452 data-model spec is in `data/vdv452-v1.5-datenmodel`.
- `tests/test_vdv.py` zips each `samples/VDV/*` subfolder and ingests it. Changes to trip timing must keep those passing.
