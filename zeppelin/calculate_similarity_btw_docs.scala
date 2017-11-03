%spark

// Loads the doc_name_word_tfidf and the doc_magnitude from Hive as DataFrames.
val docNameWordTfidfDF = sqlContext.sql("SELECT doc_name, word, tfidf FROM asanch41.hive_doc_name_word_tfidf");
val docMagnitudeDF = sqlContext.sql("SELECT doc_name, magnitude FROM asanch41.hive_doc_magnitude");
// Collects the doc_name column of the docMagnitude DF to create an array.
val docNames : Array[String] = docMagnitudeDF.select("doc_name").rdd.map(x => x.getString(0)).collect();

var similarityBtwDocs = Array[String]();
// Iterates through every doc_name in the array to calculate its similarity with all the other documents.
for (i <- 0 until 1) {
    // Get the ith document of the array.
    var docNameI : String = docNames(i);
    // Gets the list of words contained in the ith document.
    var docNameIWords : Array[String] = docNameWordTfidfDF.filter(docNameWordTfidfDF("doc_name") === docNameI)
        .select("word").rdd.map(x => x.getString(0)).collect();
    // Gets the magnitude of the ith document.
    var docNameIMagnitudeArray : Array[Double] = docMagnitudeDF.filter(docMagnitudeDF("doc_name") === docNameI).select("magnitude").rdd.map(x => x.getDouble(0))
        .collect();
    var docNameIMagnitude : Double = 1.0;
    if (docNameIMagnitudeArray.length > 0) {
        // The magnitude of the ith document was found.
        docNameIMagnitude = docNameIMagnitudeArray(0);
    }
    // Iterates over all documents beginning with the one after the ith document.
    for (j <- i + 1 until docNames.length) {
        // Get the jth document of the array.
        var docNameJ : String = docNames(j);
        // Gets the magnitude of the jth document.
        var docNameJMagnitudeArray : Array[Double] = docMagnitudeDF.filter(docMagnitudeDF("doc_name") === docNameJ).select("magnitude")
            .rdd.map(x => x.getDouble(0)).collect();
        var docNameJMagnitude : Double = 1.0;
        if (docNameJMagnitudeArray.length > 0) {
            // The magnitude of the jth document was found.
            docNameJMagnitude = docNameJMagnitudeArray(0);
        }
        // Calculates the dot product between the vectors representing the ith and jth documents.
        var dotProduct : Float = 0;
        for (word <- docNameIWords) {
            // Gets the tfidf for the tuple (document ith, word).
            var docNameIWordTfidfArray : Array[Float] = docNameWordTfidfDF
                .filter(docNameWordTfidfDF("doc_name") === docNameI && docNameWordTfidfDF("word") === word ).select("tfidf").rdd.map(x => x.getFloat(0)).collect();
            var docNameIWordTfidf : Float = 0;
            if (docNameIWordTfidfArray.length > 0) {
                // The tfidf for the tuple (document ith, word) was found.
                docNameIWordTfidf = docNameIWordTfidfArray(0);
            }
            // Gets the tfidf for the tuple (document jth, word).
            var docNameJWordTfidfArray : Array[Float] = docNameWordTfidfDF
                .filter(docNameWordTfidfDF("doc_name") === docNameJ && docNameWordTfidfDF("word") === word ).select("tfidf").rdd.map(x => x.getFloat(0)).collect();
            var docNameJWordTfidf : Float = 0;
            if (docNameJWordTfidfArray.length > 0) {
                // The tfidf for the tuple (document jth, word) was found.
                docNameJWordTfidf = docNameJWordTfidfArray(0);
            }
            dotProduct += (docNameIWordTfidf * docNameJWordTfidf);
        }
        // Calculates the Cosine similarity between the ith and jth documents using their magnitudes.
        var docNameIDocNameJSimilarity : Double = dotProduct / (docNameIMagnitude * docNameJMagnitude);
        // Creates a string to store every similarity result using a particular delimiter.
        var similarityResult : String = docNameI + ";;" + docNameJ + ";;" + docNameIDocNameJSimilarity;
        // Appends the current similarity result string to the similarity between docs list.
        similarityBtwDocs = similarityBtwDocs :+ similarityResult;
    }
}

// Stores all items in the similarity between docs list into a HDFS file.
sc.parallelize(similarityBtwDocs).coalesce(1).saveAsTextFile("hdfs:///user/asanch41/data_out/similarity_btw_docs/");