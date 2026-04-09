# Composition du groupe :
## Hicham Belghachem et Lucas Michallat

# Lien vers le github :
https://github.com/SD-hbelgha/BIG_DATA_EXAM_HADOOP/tree/main

# 1 - Combien de tags chaque film possède-t-il ?

Dans un premier temps nous allons examiner la structure du fichier pour préparer notre script python afin de répondre à la problématique.

### Récupération du fichier de données

```js
wget https://files.grouplens.org/datasets/movielens/ml-25m.zip
```

### Dézipper l'archive
```js
unzip ml-25m.zip
```

### Déplacer le fichier local dézippé vers hdfs 
```js
hdfs dfs -put /home/maria_dev/ml-25m/tags.csv /user/maria_dev/
```

## Visualiser les 10 premières lignes 
### Nous constatons que une ligne correspond à un film + un tag (indiqué par un utilisateur + timestamp)

```js
hdfs dfs -cat /user/maria_dev/tags.csv | head -n 10

userId,movieId,tag,timestamp
3,260,classic,1439472355
3,260,sci-fi,1439472256
4,1732,dark comedy,1573943598
4,1732,great dialogue,1573943604
4,7569,so bad it's good,1573943455
4,44665,unreliable narrators,1573943619
4,115569,tense,1573943077
4,115713,artificial intelligence,1573942979
4,115713,philosophical,1573943033
```

### Conception du script permettant d'obtenir le nombre de tags par films

```py
# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep

class TagsPerMovie(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_tags,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_get_tags(self, _, line):
        # Chaque ligne du fichier est découpée en parties
        # Les lignes seront distribuées entre les différents mappers
        parts = line.split(',')
        
        # On vient contrôler le nombre de parties pour chaque ligne ainsi que vérifier qu'il
        # ne sagit pas de l'en-tête
        if len(parts) == 4 and parts[0] != 'userId':
            userId, movieId, tag, timestamp = parts
            
            # Pour chaque film (movieId) la valeur 1 lui est attribué
            yield movieId, 1

    def reducer_count_tags(self, key, values):
        # Le reducer fait la somme des 1 pour chaque film (movieId)
        yield key, sum(values)

if __name__ == '__main__':
    TagsPerMovie.run()
```

## Introduction du code sur HDFS
### Nous créons un fichier vide python puis nous copions notre code au sein de ce fichier. Pour chaque script python nous viendrons procéder de la même manière
```js
nano map_reduce.py
```

## Execution du code python sur HDFS
### Nous transmettons notre script Python à Hadoop pour le lancer. Le paramètre --hadoop-streaming-jar sert de traducteur : il permet d'utiliser notre code Python au sein de l'environnement Hadoop, qui est programmé en Java.

```js
python map_reduce.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/tags.csv --output-dir hdfs:///user/maria_dev/resultats_tags
```

### Affichage du résultat :
```js
hdfs dfs -cat /user/maria_dev/resultats_tags/part-00000
```

### Extrait du résultat sur script :

#### Notre résultat est sous la forme movieId et nombre de tag pour ce même film :

```js
"99946" 2
"99957" 17
"99960" 4
"99962" 6
"99964" 9
"99968" 10
"99986" 2
"99989" 2
"99992" 7
"99996" 30
"99999" 5
```

# 2 - Combien de tags chaque utilisateur a-t-il ajoutés ?
## La logique est la même que sur la question 1

## Nous recréons un fichier python qui cette fois permet d'obtenir le nombre de tag formulé par user :

```py
# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep

class TagsPerMovie(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_tags,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_get_tags(self, _, line):
        # Chaque ligne du fichier est découpée en parties
        # Les lignes seront distribuées entre les différents mappers
        parts = line.split(',')
        
        # On vient contrôler le nombre de parties pour chaque ligne ainsi que vérifier qu'il
        # ne sagit pas de l'en-tête
        if len(parts) == 4 and parts[0] != 'userId':
            userId, movieId, tag, timestamp = parts
            
            # Pour chaque film (userId) la valeur 1 lui est attribué
            yield userId, 1

    def reducer_count_tags(self, key, values):
        # Le reducer fait la somme des 1 pour chaque utilisateur (userId) ce qui représente le nombre de tag formulé
        yield key, sum(values)

if __name__ == '__main__':
    TagsPerMovie.run()
```

## Execution du code python sur HDFS
### Nous transmettons notre script Python à Hadoop pour le lancer. Le paramètre --hadoop-streaming-jar sert de traducteur : il permet d'utiliser notre code Python au sein de l'environnement Hadoop, qui est programmé en Java.

```js
 python map_reduce2.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/tags.csv --output-dir hdfs:///user/maria_dev/resultats_tags_user
```

### Extrait du résultat sur script :

#### Notre résultat est sous la forme userId et nombre de tag pour ce même user :

```js
"155125"        7
"155129"        4
"155132"        3
"155152"        1
"155172"        1
"155179"        18
```

# 3 - Combien de blocs le fichier occupe-t-il dans HDFS dans chacune des configurations ?

### Après vérification la taille des blocs est de 128 mo
```js
hdfs dfs -stat %o /user/maria_dev/tags.csv
```
Pour la suite du TP nous devons donc réimporter le fichier dans hadoop en définissant la taille des blocs sur 64mo :

```js
134217728 octets : 128mo
```

Réimportons donc notre fichier en définissant la taille des blocs à 64mo

```js
# 64 Mo = 64 * 1024 * 1024 = 67108864 octets
hdfs dfs -D dfs.blocksize=67108864 -put /home/maria_dev/ml-25m/tags.csv /us
er/maria_dev/tags_64.csv

hdfs dfs -stat %o /user/maria_dev/tags_64.csv
67108864 # Correspond à 64mo
```

## Dans les deux cas les fichiers tiennent sur 1 block car ils sont inférieurs à 64 et 128 mo. Le système de fichiers ne va pas partitionner le fichier dans plusieurs blocks. Si leur taille était supérieure à la taille des blocks alors il aurait fallu partitionner le fichier sur plusieurs blocks.

# 4 - Combien de fois chaque tag a-t-il été utilisé pour taguer un film ?

Même principe que pour les questions précédentes

```py
# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep

class TagsPerMovie(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_tags,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_get_tags(self, _, line):

        parts = line.split(',')
        
        if len(parts) == 4 and parts[0] != 'userId':
            userId, movieId, tag, timestamp = parts
            
            yield tag, 1

    def reducer_count_tags(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    TagsPerMovie.run()
```
## Execution du code python sur HDFS
```js
python map_reduce3.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/tags_64.csv --output-dir hdfs:///user/maria_dev/resultats_tags64_tag
```
## Resultat
```js
"zombies"       1336
"zombification" 4
"zombified action"      1
"zomvies"       1
"zone"  1
"zoo"   45
"zooey deschanel"       5
"zooey deschanel's awful singing voice" 1
"zookeeper"     7
"zoologist"     2
"zoom zoom"     1
"zoom"  1
"zoophilia"     4
```

# 5 - Pour chaque film, combien de tags le même utilisateur a-t-il introduits ?
```py
# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep

class TagsPerMovie(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_tags,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_get_tags(self, _, line):
        parts = line.split(',')
        
        if len(parts) == 4 and parts[0] != 'userId':
            userId, movieId, tag, timestamp = parts
            
            # Clé = (film, utilisateur)
            yield (movieId, userId), 1

    def reducer_count_tags(self, key, values):
        # On compte combien de tags cet utilisateur a mis pour ce film
        yield key, sum(values)

if __name__ == '__main__':
    TagsPerMovie.run()
```

## Execution du code python sur HDFS
```js
python map_reduce4.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/tags_64.csv --output-dir hdfs:///user/maria_dev/resultats_tags64_film_user
```
## Resultat
```js
["99996", "100702"]     2
["99996", "104708"]     3
["99996", "105069"]     2
["99996", "153196"]     1
["99996", "157607"]     4
["99996", "19269"]      2
["99996", "48955"]      5
["99996", "53015"]      1
["99996", "56811"]      1
["99996", "6550"]       8
["99996", "65516"]      1
["99999", "6550"]       2
["99999", "99289"]      3
```
