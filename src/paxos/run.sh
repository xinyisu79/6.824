#!/bin/bash

passed="TestBasic TestDeaf TestForget"
passed2="TestManyForget TestForgetMem TestOld"
suspicious="TestRPCCount"

tests="TestManyUnreliable TestMany TestLots TestPartition "

function run_test(){
go test -test.run $1 >$1.out 2>&1
}


rm *.out
for t in $tests
do
	echo "testing $t"
	echo "done"
	for ((i=0; i < 50; i++))
	do
		run_test $t
		if ! (grep -q Passed $t.out)
		then
			echo "$t failed"
			mv $t.out $t.fail
			break
		fi
		echo -n " $i"
	done
done
