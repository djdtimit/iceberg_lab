#!/usr/bin/env bash
# Targeted run of selected test groups on all catalogs (logs in reports/targeted/).
cd /workspaces/iceberg_lab
out=reports/targeted; mkdir -p $out; rm -f $out/done
for c in lakekeeper polaris rustfs unity; do
  for t in "tests/selftest/test_lab_selftest.py tests/selftest/test_lab_adapters.py tests/selftest/test_feature_inventory.py" \
           "tests/security/test_security.py" "tests/storage/test_storage_faults.py" "tests/security/test_authz.py" \
           "tests/engines/test_types.py -k test_read_neutral" "tests/demos/test_notebooks.py" "tests/storage/test_rustfs.py"; do
    name=$(basename "$(echo "$t" | awk '{print $1}')" .py)$(echo "$t" | grep -q neutral && echo _neutral)
    CATALOG=$c REPORT_MERGE=1 timeout 3600 uv run pytest $t -p no:cacheprovider -q --tb=line > $out/${c}_$name.log 2>&1
    printf '%-11s %-28s %s | %s\n' "$c" "$name" "$(grep -E '[0-9]+ (passed|failed|skipped|errors?)( |,|$)' $out/${c}_$name.log | tail -1)" "$(grep -E '^NEW / unexpected' $out/${c}_$name.log)"
  done
done > $out/summary.txt
date > $out/done
