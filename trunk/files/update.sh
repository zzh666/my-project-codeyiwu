#!/bin/bash

myproject=my-project-codeyiwu

svn checkout http://my-project-codeyiwu.googlecode.com/svn/trunk/ $myproject

cp ./$myproject/lib/hadoop_svm.jar ./
