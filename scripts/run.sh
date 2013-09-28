#mahout seqdirectory -i ClassFile -o news-seq
mvector -i news-seq -o news-vectors -lnorm -nv -wt tfidf -x 0 -a com.chenlb.mmseg4j.analysis.MMSegAnalyzer
#mahout split -i news-vectors/tfidf-vectors --trainingOutput news-train-vectors --testOutput news-test-vectors --randomSelectionPct 40 --overwrite --sequenceFiles -xm sequential
mahout trainnb -i news-vectors/tfidf-vectors -el -o model -li labelindex -ow -c
mahout testnb -i news-vectors/tfidf-vectors -m model -l labelindex -ow -o news-testing -c
