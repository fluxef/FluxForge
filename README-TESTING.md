# Testing Guide

**First Start** (Starting DB Containers, Once)

`./run_tests.sh start`

**After Changes in init-mysql.sql or init-postgres.sql** (i.e. modificated test data)

`./run_tests.sh reset`

**Test Run** (Do the tests, Repeatedly)

`./run_tests.sh test`

**STOP**   (at the end, after all testing is finished. Delete Containers and Data)

`./run_tests.sh stop`


