#!/usr/bin/bash
#mvn install -Dmaven.test.skip=true #  disables both running the tests and compiling the tests
mvn install -DskipTests #skip running tests, but still compile them
