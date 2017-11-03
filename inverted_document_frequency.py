# coding=utf-8
import math
import os
import re
import unicodedata

from mrjob.job import MRJob
from mrjob.step import MRStep


# For each word, calculates its Inverted Document Frequency using the given documents.
class MRInvertedDocumentFrequency(MRJob):
    STOP_WORDS_ES = ["a", "actualmente", "acuerdo", "adelante", "ademas", "además", "adrede", "afirmó", "agregó", "ahi",
                     "ahora", "ahí", "al", "algo", "alguna", "algunas", "alguno", "algunos", "algún", "alli", "allí",
                     "alrededor", "ambos", "ampleamos", "antano", "antaño", "ante", "anterior", "antes", "apenas",
                     "aproximadamente", "aquel", "aquella", "aquellas", "aquello", "aquellos", "aqui", "aquél",
                     "aquélla", "aquéllas", "aquéllos", "aquí", "arriba", "arribaabajo", "aseguró", "asi", "así",
                     "atras", "aun", "aunque", "ayer", "añadió", "anadió", "aún", "b", "bajo", "bastante", "bien",
                     "breve", "buen", "buena", "buenas", "bueno", "buenos", "c", "cada", "casi", "cerca", "cierta",
                     "ciertas", "cierto", "ciertos", "cinco", "claro", "comentó", "como", "con", "conmigo", "conocer",
                     "conseguimos", "conseguir", "considera", "consideró", "consigo", "consigue", "consiguen",
                     "consigues", "contigo", "contra", "cosas", "creo", "cual", "cuales", "cualquier", "cuando",
                     "cuanta", "cuantas", "cuanto", "cuantos", "cuatro", "cuenta", "cuál", "cuáles", "cuándo", "cuánta",
                     "cuántas", "cuánto", "cuántos", "cómo", "d", "da", "dado", "dan", "dar", "de", "debajo", "debe",
                     "deben", "debido", "decir", "dejó", "del", "delante", "demasiado", "demás", "dentro", "deprisa",
                     "desde", "despacio", "despues", "después", "detras", "detrás", "dia", "dias", "dice", "dicen",
                     "dicho", "dieron", "diferente", "diferentes", "dijeron", "dijo", "dio", "donde", "dos", "durante",
                     "día", "días", "dónde", "e", "ejemplo", "el", "ella", "ellas", "ello", "ellos", "embargo",
                     "empleais", "emplean", "emplear", "empleas", "empleo", "en", "encima", "encuentra", "enfrente",
                     "enseguida", "entonces", "entre", "era", "eramos", "eran", "eras", "eres", "es", "esa", "esas",
                     "ese", "eso", "esos", "esta", "estaba", "estaban", "estado", "estados", "estais", "estamos",
                     "estan", "estar", "estará", "estas", "este", "esto", "estos", "estoy", "estuvo", "está", "están",
                     "ex", "excepto", "existe", "existen", "explicó", "expresó", "f", "fin", "final", "fue", "fuera",
                     "fueron", "fui", "fuimos", "g", "general", "gran", "grandes", "gueno", "h", "ha", "haber", "habia",
                     "habla", "hablan", "habrá", "había", "habían", "hace", "haceis", "hacemos", "hacen", "hacer",
                     "hacerlo", "haces", "hacia", "haciendo", "hago", "han", "hasta", "hay", "haya", "he", "hecho",
                     "hemos", "hicieron", "hizo", "horas", "hoy", "hubo", "i", "igual", "incluso", "indicó", "informo",
                     "informó", "intenta", "intentais", "intentamos", "intentan", "intentar", "intentas", "intento",
                     "ir", "j", "junto", "k", "l", "la", "lado", "largo", "las", "le", "lejos", "les", "llegó", "lleva",
                     "llevar", "lo", "los", "luego", "lugar", "m", "mal", "manera", "manifestó", "mas", "mayor", "me",
                     "mediante", "medio", "mejor", "mencionó", "menos", "menudo", "mi", "mia", "mias", "mientras",
                     "mio", "mios", "mis", "misma", "mismas", "mismo", "mismos", "modo", "momento", "mucha", "muchas",
                     "mucho", "muchos", "muy", "más", "mí", "mía", "mías", "mío", "míos", "n", "nada", "nadie", "ni",
                     "ninguna", "ningunas", "ninguno", "ningunos", "ningún", "no", "nos", "nosotras", "nosotros",
                     "nuestra", "nuestras", "nuestro", "nuestros", "nueva", "nuevas", "nuevo", "nuevos", "nunca", "o",
                     "ocho", "os", "otra", "otras", "otro", "otros", "p", "pais", "para", "parece", "parte", "partir",
                     "pasada", "pasado", "paìs", "peor", "pero", "pesar", "poca", "pocas", "poco", "pocos", "podeis",
                     "podemos", "poder", "podria", "podriais", "podriamos", "podrian", "podrias", "podrá", "podrán",
                     "podría", "podrían", "poner", "por", "porque", "posible", "primer", "primera", "primero",
                     "primeros", "principalmente", "pronto", "propia", "propias", "propio", "propios", "proximo",
                     "próximo", "próximos", "pudo", "pueda", "puede", "pueden", "puedo", "pues", "q", "qeu", "que",
                     "quedó", "queremos", "quien", "quienes", "quiere", "quiza", "quizas", "quizá", "quizás", "quién",
                     "quiénes", "qué", "r", "raras", "realizado", "realizar", "realizó", "repente", "respecto", "s",
                     "sabe", "sabeis", "sabemos", "saben", "saber", "sabes", "salvo", "se", "sea", "sean", "segun",
                     "segunda", "segundo", "según", "seis", "ser", "sera", "será", "serán", "sería", "señaló", "senaló",
                     "si", "sido", "siempre", "siendo", "siete", "sigue", "siguiente", "sin", "sino", "sobre", "sois",
                     "sola", "solamente", "solas", "solo", "solos", "somos", "son", "soy", "soyos", "su", "supuesto",
                     "sus", "suya", "suyas", "suyo", "sé", "sí", "sólo", "t", "tal", "tambien", "también", "tampoco",
                     "tan", "tanto", "tarde", "te", "temprano", "tendrá", "tendrán", "teneis", "tenemos", "tener",
                     "tenga", "tengo", "tenido", "tenía", "tercera", "ti", "tiempo", "tiene", "tienen", "toda", "todas",
                     "todavia", "todavía", "todo", "todos", "total", "trabaja", "trabajais", "trabajamos", "trabajan",
                     "trabajar", "trabajas", "trabajo", "tras", "trata", "través", "tres", "tu", "tus", "tuvo", "tuya",
                     "tuyas", "tuyo", "tuyos", "tú", "u", "ultimo", "un", "una", "unas", "uno", "unos", "usa", "usais",
                     "usamos", "usan", "usar", "usas", "uso", "usted", "ustedes", "v", "va", "vais", "valor", "vamos",
                     "van", "varias", "varios", "vaya", "veces", "ver", "verdad", "verdadera", "verdadero", "vez",
                     "vosotras", "vosotros", "voy", "vuestra", "vuestras", "vuestro", "vuestros", "w", "x", "y", "ya",
                     "yo", "z", "él", "ésa", "ésas", "ése", "ésos", "ésta", "éstas", "éste", "éstos", "última",
                     "últimas", "último", "últimos"]
    STOP_WORDS_EN = ["a", "a's", "able", "about", "above", "according", "accordingly", "across", "actually", "after",
                     "afterwards", "again", "against", "ain't", "all", "allow", "allows", "almost", "alone", "along",
                     "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "another", "any",
                     "anybody", "anyhow", "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear",
                     "appreciate", "appropriate", "are", "aren't", "around", "as", "aside", "ask", "asking",
                     "associated", "at", "available", "away", "awfully", "b", "be", "became", "because", "become",
                     "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "believe", "below",
                     "beside", "besides", "best", "better", "between", "beyond", "both", "brief", "but", "by", "c",
                     "c'mon", "c's", "came", "can", "can't", "cannot", "cant", "cause", "causes", "certain",
                     "certainly", "changes", "clearly", "co", "com", "come", "comes", "concerning", "consequently",
                     "consider", "considering", "contain", "containing", "contains", "corresponding", "could",
                     "couldn't", "course", "currently", "d", "definitely", "described", "despite", "did", "didn't",
                     "different", "do", "does", "doesn't", "doing", "don't", "done", "down", "downwards", "during", "e",
                     "each", "edu", "eg", "eight", "either", "else", "elsewhere", "enough", "entirely", "especially",
                     "et", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex",
                     "exactly", "example", "except", "f", "far", "few", "fifth", "first", "five", "followed",
                     "following", "follows", "for", "former", "formerly", "forth", "four", "from", "further",
                     "furthermore", "g", "get", "gets", "getting", "given", "gives", "go", "goes", "going", "gone",
                     "got", "gotten", "greetings", "h", "had", "hadn't", "happens", "hardly", "has", "hasn't", "have",
                     "haven't", "having", "he", "he's", "hello", "help", "hence", "her", "here", "here's", "hereafter",
                     "hereby", "herein", "hereupon", "hers", "herself", "hi", "him", "himself", "his", "hither",
                     "hopefully", "how", "howbeit", "however", "i", "i'd", "i'll", "i'm", "i've", "ie", "if", "ignored",
                     "immediate", "in", "inasmuch", "inc", "indeed", "indicate", "indicated", "indicates", "inner",
                     "insofar", "instead", "into", "inward", "is", "isn't", "it", "it'd", "it'll", "it's", "its",
                     "itself", "j", "just", "k", "keep", "keeps", "kept", "know", "known", "knows", "l", "last",
                     "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "let's", "like", "liked",
                     "likely", "little", "look", "looking", "looks", "ltd", "m", "mainly", "many", "may", "maybe", "me",
                     "mean", "meanwhile", "merely", "might", "more", "moreover", "most", "mostly", "much", "must", "my",
                     "myself", "n", "name", "namely", "nd", "near", "nearly", "necessary", "need", "needs", "neither",
                     "never", "nevertheless", "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor",
                     "normally", "not", "nothing", "novel", "now", "nowhere", "o", "obviously", "of", "off", "often",
                     "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto", "or", "other", "others",
                     "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "own", "p",
                     "particular", "particularly", "per", "perhaps", "placed", "please", "plus", "possible",
                     "presumably", "probably", "provides", "q", "que", "quite", "qv", "r", "rather", "rd", "re",
                     "really", "reasonably", "regarding", "regardless", "regards", "relatively", "respectively",
                     "right", "s", "said", "same", "saw", "say", "saying", "says", "second", "secondly", "see",
                     "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible", "sent",
                     "serious", "seriously", "seven", "several", "shall", "she", "should", "shouldn't", "since", "six",
                     "so", "some", "somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat",
                     "somewhere", "soon", "sorry", "specified", "specify", "specifying", "still", "sub", "such", "sup",
                     "sure", "t", "t's", "take", "taken", "tell", "tends", "th", "than", "thank", "thanks", "thanx",
                     "that", "that's", "thats", "the", "their", "theirs", "them", "themselves", "then", "thence",
                     "there", "there's", "thereafter", "thereby", "therefore", "therein", "theres", "thereupon",
                     "these", "they", "they'd", "they'll", "they're", "they've", "think", "third", "this", "thorough",
                     "thoroughly", "those", "though", "three", "through", "throughout", "thru", "thus", "to",
                     "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying",
                     "twice", "two", "u", "un", "under", "unfortunately", "unless", "unlikely", "until", "unto", "up",
                     "upon", "us", "use", "used", "useful", "uses", "using", "usually", "uucp", "v", "value", "various",
                     "very", "via", "viz", "vs", "w", "want", "wants", "was", "wasn't", "way", "we", "we'd", "we'll",
                     "we're", "we've", "welcome", "well", "went", "were", "weren't", "what", "what's", "whatever",
                     "when", "whence", "whenever", "where", "where's", "whereafter", "whereas", "whereby", "wherein",
                     "whereupon", "wherever", "whether", "which", "while", "whither", "who", "who's", "whoever",
                     "whole", "whom", "whose", "why", "will", "willing", "wish", "with", "within", "without", "won't",
                     "wonder", "would", "wouldn't", "x", "y", "yes", "yet", "you", "you'd", "you'll", "you're",
                     "you've", "your", "yours", "yourself", "yourselves", "z", "zero"]

    SPECIAL_CHARACTERS_RE = re.compile("[^A-Za-z]+")

    MAIN_WORDS = ["a", "mas", "que", "el", "habia", "project", "o", "no", "casa", "don", "si", "esta", "dios",
                  "hombre", "y", "vida", "asi", "gutenbergtm", "f", "despues", "m", "senor", "tenia", "ojos", "work",
                  "e", "padre", "dia", "aqui", "d", "tambien", "tierra", "mujer", "alli", "mi", "fue", "noche", "mano",
                  "solo", "mundo", "anos", "hombres", "habian", "como", "es", "works", "cabeza", "gutenberg", "cosa",
                  "rey", "pueblo", "amor", "juan", "se", "voz", "yo", "ella", "hijo", "pues", "tu", "de", "dona", "v",
                  "manos", "alma", "nombre", "electronic", "puerta", "n", "la", "madre", "joven", "senora", "dias",
                  "paso", "pero", "podia", "iba", "ciudad", "palabras", "corazon", "agua", "gente", "hacia", "dijo",
                  "camino", "por", "usted", "punto", "bien", "muerte", "quien", "foundation", "grande", "luz", "san",
                  "mil", "este", "pobre", "visto"]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_occurrence_for_word_and_doc_name,
                   reducer=self.reducer_calculate_idf_for_word)
        ]

    # Yields [word, document name] for each word in the line.
    def mapper_get_occurrence_for_word_and_doc_name(self, _, line):
        # Gets the input file name.
        try:
            doc_name = os.getenv('mapreduce_map_input_file')
        except KeyError:
            doc_name = os.getenv('map_input_file')

        # In order to yield a pair, the word has to pass the validation filter.
        for word in line.split():
            try:
                # Converts the word into unicode in order to make next transformations.
                norm_word = unicode(word, "iso-8859-1")
                norm_word = norm_word.lower()
                # Verifies that the word until then is not a stop word.
                if norm_word not in self.STOP_WORDS_ES and norm_word not in self.STOP_WORDS_EN:
                    # Normalizes the unicode word to the 'Normal Form Composed' i.e. replaces accent letters with non
                    # accented ones.
                    norm_word = unicodedata.normalize('NFD', norm_word).encode('ascii', 'ignore')
                    # Removes every special character from the normalized word.
                    norm_word = re.sub(self.SPECIAL_CHARACTERS_RE, '', norm_word)
                    # Makes possible remaining uppercase letters lowercase ones.
                    norm_word = norm_word.lower()
                    # Verifies that the resulting normalized word is not an empty string.
                    if norm_word != "" and norm_word not in self.STOP_WORDS_ES and norm_word not in self.STOP_WORDS_EN:
                        if norm_word in self.MAIN_WORDS:
                            # Yields the word after the filtering.
                            yield norm_word, doc_name
            except:
                # There was a problem filtering the word and it is discarded thus.
                None

    # Prints [word, inverted document frequency] for each word.
    def reducer_calculate_idf_for_word(self, word, doc_names):
        # Iterates over the doc_names (Generator) to insert them in a set doc_names and get the unique doc_names.
        unique_doc_name_list = set()
        for doc_name in doc_names:
            unique_doc_name_list.add(doc_name)

        # Calculates the Inverted Document Frequency using its formula. The denominator must be cast to float in order
        # to obtain a floating point division. len(unique_doc_name_list) is the number of documents in which the word
        # appears.
        inverted_document_frequency = math.log10(self.TOTAL_NUMB_OF_DOCUMENTS / float(len(unique_doc_name_list)))

        # Formats the output. ';;' is selected to separate the word from its inverted document frequency.
        row = word + ";;" + ('%.6f' % inverted_document_frequency)
        print row


if __name__ == '__main__':
    MRInvertedDocumentFrequency.run()
