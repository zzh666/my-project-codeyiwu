#!/bin/bash

#split train file
classpath=./hadoop_svm.jar
execlass=hadoop.tools.Main
trainfile=./my-project-codeyiwu/exp/a9a
filename=a9a
splitnum=8

echo classpath=$classpath 	execlass=$execlass 
echo trainfile=$trainfile 	splitnum=$splitnum 
java -classpath $classpath $execlass a $trainfile $splitnum 

DATE=$(date "+%Y%m%d%H%M%S")
exp=exp-$DATE

trainfolder=$trainfile\_splits/
mkdir $exp
cp -a $trainfolder ./$exp
echo upload ./$exp/ to ./$exp/
./bin/hadoop fs -copyFromLocal ./$exp ./$exp
#./bin/hadoop fs -ls ./$exp

inpath=./$exp/$filename\_splits/
outpath=./$exp/$filename\_splits-output/

echo exe hadoop hadoop.svm.MRSVMTrainnFilt $inpath $outpath
./bin/hadoop jar ./hadoop_svm.jar hadoop.svm.MRSVMTrainnFilt "/tmp" $inpath $outpath

echo download $outpath from ./$exp/
./bin/hadoop fs -copyToLocal $outpath ./$exp/

ls $outpath
outmodel=$outpath/mod/
mkdir $outmodel
for fp in $(cat $outpath/part-00000 | awk '{print $1}')
do
	echo download $fp to directory $outmodel
	./bin/hadoop fs -copyToLocal $fp $outmodel
done

echo finish
