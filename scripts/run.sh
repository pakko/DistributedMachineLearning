#mahout seqdirectory -i ClassFile -o news-seq
mvector -i news-seq -o news-vectors -lnorm -nv -wt tfidf -a com.chenlb.mmseg4j.analysis.MMSegAnalyzer
#mahout split -i news-vectors/tfidf-vectors --trainingOutput news-train-vectors --testOutput news-test-vectors --randomSelectionPct 40 --overwrite --sequenceFiles -xm sequential
mahout trainnb -i news-vectors/tfidf-vectors -el -o model -li labelindex -ow -c
mahout testnb -i news-vectors/tfidf-vectors -m model -l labelindex -ow -o news-testing -c

mkmeans --input news-vectors/tfidf-vectors --numClusters 10 --maxIter 10 --output kmeans -t1 3 -t2 6

mahout clusterdump -i kmeans/clusters-10-final --pointsDir kmeans/clusteredPoints -o result.txt

mahout kmeans -i news-vectors/tfidf-vectors/ -c kmeans-centroids -cl -o kmeans-clusters -k 10 -ow -x 10 -dm org.apache.mahout.common.distance.CosineDistanceMeasure

mahout clusterdump -d news-vectors/dictionary.file-0 -dt sequencefile -i kmeans-clusters/clusters-2-final -n 20 -b 100 -o cdump.txt -p kmeans-clusters/clusteredPoints/