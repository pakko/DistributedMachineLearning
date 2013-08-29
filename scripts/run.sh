mahout seqdirectory -i ClassFile -o news-seq
mvector -i news-seq -o news-vectors -lnorm -nv -wt tfidf -a com.chenlb.mmseg4j.analysis.MMSegAnalyzer
mahout split -i news-vectors/tfidf-vectors --trainingOutput news-train-vectors --testOutput news-test-vectors --randomSelectionPct 40 --overwrite --sequenceFiles -xm sequential
mahout trainnb -i news-train-vectors -el -o model -li labelindex -ow -c
mahout testnb -i news-test-vectors -m model -l labelindex -ow -o news-testing -c
