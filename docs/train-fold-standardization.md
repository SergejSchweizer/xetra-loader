# Train-fold-local standardization

`xetra_loader.xetra_features` is a global serving view. It contains the two
approved levels and derived feature values, but it never contains fit means,
standard deviations, or fold-dependent values.

Downstream training code must use
`xetra_loader.features.standardization.standardize_train_fold` with an explicit
`FeatureFold`. The fold is a disjoint partition of complete feature-view row
identities `(isin, exchange, code, trade_date)` into `train`, `validation`, and
`test`. The adapter rejects rows outside or missing from that partition.

The adapter fits each selected catalog-derived feature on non-NULL training
values only, using the population standard deviation. The fitted statistics are
then applied unchanged to all three partitions. A missing or zero-variance
training feature is reported in `FeatureStatistics` and produces `None`, never
a fabricated standardized value. Validation and test rows cannot influence
the fit.

Example:

```python
from xetra_loader.features.standardization import (
    FeatureFold,
    standardize_train_fold,
)

result = standardize_train_fold(rows, FeatureFold(train=train_keys,
                                                   validation=validation_keys,
                                                   test=test_keys))
```

`source_fingerprint` covers the selected source values only. It is evidence for
repeatability and is not part of Gold fingerprints or the PostgreSQL feature
view. Consumers should persist the returned statistics with their training
artifact, not write them back to PostgreSQL.
