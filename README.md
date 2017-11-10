# Information Retrieval and Clustering using the Project Gutenberg dataset.

Using some techniques learned in the Parallel Computing course at Universidad EAFIT, the Processing phase of a Text Analysis app for Information Retrieval and Clustering was implemented using the Gutenberg-Project dataset (eBooks in english and spanish).

## Table of contents
  * [Dependencies.](#dependencies)
  * [Clarifications.](#clarifications)
  * [Usage.](#usage)

## Dependencies
- [Python] 2.7.10
- [mrjob] 0.5.10
- [redis] 2.10.6
- [Pig] 0.17.0
- [Spark] 2.2.0

## Clarifications
  - While processing every document of the Project Gutenberg dataset, each word is filtered to make it lowercase, check if it is not a spanish or english stopword ([stopwords-json]), remove accents and delete all special characters.
  - After the filtering, a word is only selected to be part of the processing if it is one of the 100 main words.
  - The ``main_words.py`` file is used to find the 100 main words regarding the whole dataset.
  - The ``norm_term_frequency.py`` file counts the number of ocurrences of every main word in every document and normalizes that number using its total number of terms.
  - The ``inverted_document_frequency.py`` file calculates a number for each main word using the formula **log(TOTAL_NUMB_OF_DOCUMENTS / NUMB_OF_DOCUMENTS_CONTAINING_THE_WORD)**.
  - The ``documents_containing_word.py`` file builds, for every main word, a list of the documents where that word appears. The list elements are sorted in descendant order using the word ocurrences as criterion.
  - The ``pig/calculate_tfidf.pig`` file uses the outputs of the ``norm_term_frequency.py`` and ``inverted_document_frequency.py`` files to join them by word and calculate the _tfidf_ value for each document-word pair.
  - The ``pig/calculate_magnitude.pig`` file uses the output of the ``pig/calculate_tfidf.pig`` script to calculate the magnitude of every document using the _tfidf_ values.
  - The ``zeppelin/calculate_similarity_btw_docs.scala`` file uses the outputs of the ``pig/calculate_tfidf.pig`` and  ``pig/calculate_magnitude.pig`` scripts to calculate the _Cosine similarity_ between every pair of documents. That is necessary to determine which are the more related documents given a specific one.
  - The ``idf_by_word.py`` file is used to transfer the output of the ``inverted_document_frequency.py`` file to a remote Redis database.
  - The ``word_in_docs.py`` file is used to transfer the output of the ``documents_containing_word.py`` file to a remote Redis database.
  - The ``tfidf_for_word_in_docs.py`` file is used to transfer the output of the ``pig/calculate_tfidf.pig`` script to a remote Redis database.
  - The ``magnitude_by_doc.py`` file is used to transfer the output of the ``pig/calculate_magnitude.pig`` script to a remote Redis database.
  - The ``similarity_doc.py`` file is used to transfer the output of the ``zeppelin/calculate_similarity_btw_docs.scala`` file to a remote Redis database.
 - The remote Redis database is accessed on a web application ([gutenberg-web]) that was designed for the online part of this Information Retrieval and Clustering System.

#
> The following section assumes that the whole dataset and the _Python_, _Pig_ and _Scala_ files reside on an environment with [Hortonworks 2.5] installed. It makes possible to execute the implemented jobs in _Hadoop_.
#
> The dataset must be in HDFS and the path _user/asanch41/data_out/_ must exist there.
#
> The current directory on the terminal should be where the mentioned _Python_ files are located.

### Usage
1. Execute the ``norm_term_frequency.py`` file by typing on the terminal:

    ```sh
    $ python norm_term_frequency.py hdfs://path/to/all/documents/*.txt -r hadoop --output-dir hdfs:///user/asanch41/data_out/norm_term_frequency
    ```

2. Execute the ``inverted_document_frequency.py`` file by typing on the terminal:

    ```sh
    $ python inverted_document_frequency.py hdfs://path/to/all/documents/*.txt -r hadoop --output-dir hdfs:///user/asanch41/data_out/inverted_document_frequency
    ```

3. Execute the ``documents_containing_word.py`` file by typing on the terminal:

    ```sh
    $ python documents_containing_word.py hdfs://path/to/all/documents/*.txt -r hadoop --output-dir hdfs:///user/asanch41/data_out/documents_containing_word
    ```

4. Go to the _Hive view_ on your _Hortonworks_ environment, create a database with the name **asanch41**, select it and then execute the following two queries:
    ```sh
    CREATE EXTERNAL TABLE hive_doc_name_word_tfidf (doc_name STRING, word STRING, tfidf FLOAT);
    ```
    ```sh
    CREATE EXTERNAL TABLE hive_doc_magnitude (doc_name STRING, magnitude DOUBLE);
    ```
    > This step is important because the ``pig/calculate_tfidf.pig`` and ``pig/calculate_magnitude.pig`` files will use the created tables to store their output, which will then be used by the ``zeppelin/calculate_similarity_btw_docs.scala`` file during its execution.

5. Go to the _Pig view_ on your _Hortonworks_ environment, create two scripts using the ``pig/calculate_tfidf.pig`` and ``pig/calculate_magnitude.pig`` file content and execute them from there in that order.

6. Go to the _Zeppelin Notebook_ on your _Hortonworks_ environment, create a new note by pasting the content of the ``zeppelin/calculate_similarity_btw_docs.scala`` file and run it from there.

7. When you complete all previous steps, execute the following commands to send the processed data to the Azure Redis DB that serves the [Gutenberg Information Retrieval] application:
    ```sh
    $ python idf_by_word.py hdfs:///user/asanch41/data_out/inverted_document_frequency/part-00000 -r hadoop --output-dir hdfs:///user/asanch41/data_out/inverted_document_frequency_redis
    ```
    ```sh
    $ python word_in_docs.py hdfs:///user/asanch41/data_out/documents_containing_word/part-00000 -r hadoop --output-dir hdfs:///user/asanch41/data_out/documents_containing_word_redis
    ```
    ```sh
    $ python tfidf_from_word_in_docs.py hdfs:///user/asanch41/data_out/doc_name_word_tfidf/part-r-00000 -r hadoop --output-dir hdfs:///user/asanch41/data_out/doc_name_word_tfidf_redis
    ```
    ```sh
    $ python magnitude_by_doc.py hdfs:///user/asanch41/data_out/doc_magnitude/part-r-00000 -r hadoop --output-dir hdfs:///user/asanch41/data_out/doc_magnitude_redis
    ```
    ```sh
    $ python similarity_doc.py hdfs:///user/asanch41/data_out/similarity_btw_docs/part-00000 -r hadoop --output-dir hdfs:///user/asanch41/data_out/similarity_btw_docs_redis
    ```

8. Finally, open the [Gutenberg Information Retrieval] application on the web. Enter a query and look for documents and their communities.

[Python]: <https://www.python.org/downloads/>
[mrjob]: <https://pythonhosted.org/mrjob/>
[redis]: <https://pypi.python.org/pypi/redis>
[Pig]: <https://pig.apache.org/>
[Spark]: <https://spark.apache.org/>
[stopwords-json]: <https://github.com/6/stopwords-json>
[gutenberg-web]: <https://github.com/jcarmon4/gutenberg-web>
[Hortonworks 2.5]: <https://es.hortonworks.com/>
[Gutenberg Information Retrieval]: <http://gutenberg-ir.herokuapp.com/>