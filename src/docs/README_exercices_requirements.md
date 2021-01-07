# Exercices Data engineer / Spark
Les exercices sont à réaliser sur Spark >=2.4.7.
Résultats à générer sous le format parquet ou Avro + expliquer le choix.
Pour chaque exercice, penser au fait que le code développé doit pouvoir être repris tel quel et mis en production + expliquer l`approche.

## Exercice 01 - Conversion d`un CSV vers un DataFrame
- Input(s):
  - un fichier CSV `/test_technique/inputs/01_csv_to_dataframe.csv`

- Objectif(s):
  - générer un DataFrame avec les champs castés (String/Int/TimeStamp/..)
  - les n° de téléphone propres (numéros seulement)
  - les nombres à `,` précis
  - le champs JSON éclaté en plusieurs colonnes (`guid` et `poi`)

## Exercice 02 - Comptage des Clients et Contrats par Portefeuille
- Input(s):
  - un fichier PARQUET `/test_technique/inputs/02_campagne.parquet.csv`
    nb: les champs qui nous intéressent sont
    * `NMPTF` (n° Portefeuille)
    * `00021_NUMCLIA` (n° Client)
    * `00004_NUMCLE` (n° Contrat)
    * `01255_MOISAN` (mois echéance)

- Objectif(s):
  - générer un DataFrame qui nous donne par Portefeuille:
   - le nombre total de clients du Portefeuille
   - le nombre total de contrats du Portefeuille
   - des colonnes qui nous donnent, sur les mois d`echéance 1/2/11/12, le nombre total de contrats dont le champs (voir champs 01255_MOISAN)
   - schéma attendu attendu: `NMPTF`, `TOTAL_CLIENTS`, `TOTAL_CONTRATS`, `ECH_1`, `ECH_2`, `ECH_`, `ECH4`

## Exercice 03 - Top 3
- Input(s):
  - reprendre le résultat de l`exercice 02

- Objectif(s):
  - sortir, pour chaque portefeuille, le top 3 sur base du nombre de clients
 