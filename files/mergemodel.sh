#!/bin/bash

classpath=./hadoop_svm.jar
execlass=hadoop.tools.Main
mergefolder=exp-20120412195624/a9a_splits-output/mod/
suffix=model

echo classpath=$classpath 	execlass=$execlass 
echo mergefolder=$mergefolder 	suffix=$suffix
java -classpath $classpath $execlass b $mergefolder $suffix

cp $mergefolder/merge.model_ $mergefolder/../../
