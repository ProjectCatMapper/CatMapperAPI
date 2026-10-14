# Admin USES Parent Immediate Processing

When Admin edits or deletes a USES property, the database mutation is selected by the relationship's `relID`. The previous code sent the CMID from the form to `processUSES`, without verifying that the selected relationship belonged to that category. An outdated asynchronous Admin lookup could also replace the tie choices after the CMID changed. That creates a code path where the property write and immediate derived `CONTAINS` refresh target different categories; the edit can appear successful while the intended category stays queued for the nightly routine.

This is a reproducible code-level failure mode, but no specific live edit was available to confirm it caused the reported incident.

The Admin path now checks the selected relationship and category together before property validation or mutation. The edit processes the category CMID returned by `updateProperty`; property deletion returns and processes the category CMID from the relationship whose property changed. A missing or mismatched selection is rejected before a write. In the UI, obsolete USES lookup responses are ignored, and a newly loaded relationship list clears the selected tie index so an old index cannot select a different relationship.

Regression coverage is in `tests/admin_edit_uses_test.py` for pre-write mismatch rejection and processing returned category CMIDs, and `CatMapperJS/src/components/Admin.usesLookup.test.jsx` for out-of-order lookup responses. Verified with `conda run -n api_env python -m pytest tests/admin_edit_uses_test.py -q` and `npm test -- --run src/components/Admin.usesLookup.test.jsx`.
