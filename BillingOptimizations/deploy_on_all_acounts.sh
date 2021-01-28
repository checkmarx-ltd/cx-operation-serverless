#!/bin/bash
echo "*******************	SAST	***********************"

echo "sls deploy --aws-profile default"
sls deploy --aws-profile default
echo "sls deploy --aws-profile 359856697693"
sls deploy --aws-profile 359856697693
echo "sls deploy --aws-profile 544428519067"
sls deploy --aws-profile 544428519067
echo "sls deploy --aws-profile 425154196092"
sls deploy --aws-profile 425154196092
echo "sls deploy --aws-profile 238450497947"
sls deploy --aws-profile 238450497947
echo "sls deploy --aws-profile 185449903594"
sls deploy --aws-profile 185449903594

echo "*******************	CB	  ***********************"

echo "sls deploy --aws-profile 379959622371_Lambda_FullAccess"
sls deploy --aws-profile 379959622371_Lambda_FullAccess
echo "sls deploy --aws-profile 267564311969_Lambda_FullAccess"
sls deploy --aws-profile 267564311969_Lambda_FullAccess
echo "sls deploy --aws-profile 759786165482_Lambda_FullAccess"
sls deploy --aws-profile 759786165482_Lambda_FullAccess
echo "sls deploy --aws-profile 771250916586_Lambda_FullAccess"
sls deploy --aws-profile 771250916586_Lambda_FullAccess


echo "*******************	SCA	  ***********************"

echo "sls deploy --aws-profile 006765415138_Lambda_FullAccess"
sls deploy --aws-profile 006765415138_Lambda_FullAccess
echo "sls deploy --aws-profile 666740670058_Lambda_FullAccess"
sls deploy --aws-profile 666740670058_Lambda_FullAccess


echo "*******************	Architecture	  ***********************"

echo  "sls deploy --aws-profile 881053136306_Lambda_FullAccess"
sls deploy --aws-profile 881053136306_Lambda_FullAccess


