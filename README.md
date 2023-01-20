# db_unified
Bibliothèque python permettant d'accéder à de nombreuses bases de données SQL différentes avec les mêmes fonctions
De plus, quelques améliorations sont disponibles telles que la récupération des noms de colonne en plus des valeurs

Bases de données actuellement supportées
- postgresql
- mysql
- mariadb
- sql server

## Installation
Installez les dépendances avec pip install -r requirements.txt

ATTENTION, pour mariadb certaines dépendances supplémentaires sont nécessaires. 
Vous pouvez les installer avec `sudo apt-get install libmariadb3 libmariadb-dev`