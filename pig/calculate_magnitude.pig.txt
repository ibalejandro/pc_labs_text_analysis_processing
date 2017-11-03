-- Loads the doc_name_word_tfidf from Hive.
doc_name_word_tfidf = LOAD '/user/asanch41/data_out/doc_name_word_tfidf' USING org.apache.pig.piggybank.storage.MyRegExLoader('([^\\:]+);;([^\\:]+);;([^\\:]+)') AS (doc_name:chararray, word:chararray, tfidf:float);
-- Squares the tfidf value and generates another column.
squared_tfidf = FOREACH doc_name_word_tfidf GENERATE $0 AS doc_name, (tfidf * tfidf) AS sq_tfidf;
-- Groups the squared tfidfs by the doc_name.
group_squared_tfidf_by_doc_name = GROUP squared_tfidf BY doc_name;
-- Sums up all squared tfidfs into a single value.
sum_squared_tfidf = FOREACH group_squared_tfidf_by_doc_name GENERATE group AS doc_name, SUM(squared_tfidf.sq_tfidf) AS sum_sq_tfidf;
-- Takes the square root of the sum of squared tfidfs.
doc_magnitude_table = FOREACH sum_squared_tfidf GENERATE $0 AS doc_name, SQRT($1) AS magnitude;
-- Creates a string to store every resulting row using a particular delimiter into HDFS.
doc_magnitude_table_with_delim = FOREACH doc_magnitude_table generate CONCAT(doc_name, ';;', (chararray)magnitude);
-- Stores the result into HDFS and Hive.
STORE doc_magnitude_table_with_delim INTO '/user/asanch41/data_out/doc_magnitude';
STORE doc_magnitude_table INTO 'ASANCH41.hive_doc_magnitude' USING org.apache.hive.hcatalog.pig.HCatStorer();
