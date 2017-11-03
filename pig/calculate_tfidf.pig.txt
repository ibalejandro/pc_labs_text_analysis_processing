-- Loads the norm_term_frequency from Hive.
norm_tf = LOAD '/user/asanch41/data_out/norm_term_frequency/part-00000' USING org.apache.pig.piggybank.storage.MyRegExLoader('([^\\:]+);;([^\\:]+);;([^\\:]+)') AS (doc_name:chararray, word:chararray, ntf:float);
-- Loads the inverted_document_frequency from Hive.
inv_df = LOAD '/user/asanch41/data_out/inverted_document_frequency/part-00000' USING org.apache.pig.piggybank.storage.MyRegExLoader('([^\\:]+);;([^\\:]+)') AS (word:chararray, idf:float);
-- Joins the two tables using the word as key.
join_norm_tf_inv_df = JOIN norm_tf BY word, inv_df BY word;
-- Generates another column with the product of ntf and idf (i.e. tfidf).
tfidf_table = FOREACH join_norm_tf_inv_df GENERATE $0 AS doc_name, $1 AS word, ($2 * $4) AS tfidf;
-- Creates a string to store every resulting row using a particular delimiter into HDFS.
tfidf_table_with_delim = FOREACH tfidf_table generate CONCAT(doc_name, ';;', word, ';;', (chararray)tfidf);
-- Stores the result into HDFS and Hive.
STORE tfidf_table_with_delim INTO '/user/asanch41/data_out/doc_name_word_tfidf';
STORE tfidf_table INTO 'ASANCH41.hive_doc_name_word_tfidf' USING org.apache.hive.hcatalog.pig.HCatStorer();
