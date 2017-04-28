#!/bin/bash
#

DIR=`dirname $0`

# Run server:stmgr_unittest
for i in `seq 1 100`; do
  echo === repeat test $i ===
  bazel --bazelrc=tools/travis-ci/bazel.rc test --config=ubuntu --nocache_test_results --test_tag_filters=flaky heron/...
  RESULT=$?
  if [ $RESULT -ne 0 ]; then
    # Dump out stream manager log
    echo "DUMPING STMGR TEST LOG"
    # tail -n +1 testlogs/heron/stmgr/tests/cpp/server/stmgr_unittest/test.log
    exit 1
  fi
done
