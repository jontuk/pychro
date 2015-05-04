#!/bin/sh
mvn exec:java -Dexec.mainClass=uk.me.jpt.pychro.test.setup.Application -Dexec.args="$*"
