from pythonping import ping
import logging as log


class db_unified:
	""" Classe pour la gestion de la DB """

	def __init__(self, db_type = None, db_name=None, db_server=None, db_port=None, db_user=None, db_password = None, sslmode=None, options = None, config=None):
		""" db_type : type de db, valeurs possibles : 
				- postgresql
				- mariadb
				- mysql
				- sqlserver

			sslmode | valeurs possibles: disable, allow, prefer, require, verify-ca, verify-full 
			options peut servir à chercher dans un schéma particulier : options="-c search_path=dbo,public")
			config: Premet de passer toute la config via un dictionnaire. Les clés sont:
				- type
				- name
    			- addr
    			- port
    			- user
    			- passwd
    		Une config passée en paramètre écrase les valeurs par défaut du type de base de donnée
    		Un paramètre passé en paramètre en plus d'une config écrase le paramètre correspondant de la config


		"""

		self.database = None
		self.host = None
		self.port = None
		self.user = None
		self.password = None
		self.sslmode = None
		self.options = None

		# On vérifie que le type de db a été spécifié (en paramètre ou dans la config)
		if db_type is None and (config is not None and config.get("type") is None):
			raise ValueError("Le type de base de données utilisé n'a pas été spécifié (paramètre 'db_type')")

		# On sauve le type de db
		self.db_type = db_type if db_type is not None else config.get("type")

		# Import des bibliothèques en fonction du type de db choisi
		if self.db_type == 'postgresql':
			global psycopg2
			import psycopg2
			import psycopg2.extras
			global platform
			import platform
		elif self.db_type == 'mariadb':
			global mariadb
			import mariadb # Installer avec 'pip install mariadb'. Il y aura peut-�tre besoin de certaines d�pendances 'sudo apt-get install libmariadb3 libmariadb-dev'
		elif self.db_type == 'mysql':
			global mysql
			import mysql.connector
		elif self.db_type == 'sqlserver':
			global pyodbc
			import pyodbc
			global struct
			import struct

		# Attribution des valeurs par défaut en fonction du type de db
		if self.db_type == "postgresql":
			self.port = 5432
			self.user = "postgres"
			self.sslmode = "allow"
			self.options = ""
		elif self.db_type == "mariadb":
			self.port = 3306
			self.sslmode = "allow"
			self.options = ""
		elif self.db_type == "mysql":
			self.port = 3306
			self.sslmode = "allow"
			self.options = ""
		elif self.db_type == "sqlserver":
			self.port = ""
			self.sslmode = "allow"
			self.options = ""

		# On récupère les éléments de la config s'ils existent
		if config is not None:
			self.database = config.get("name", self.database)
			self.host = config.get("addr", self.host)
			self.port = config.get("port", self.port)
			self.user = config.get("user", self.user)
			self.password = config.get("passwd", self.password)
			self.sslmode = config.get("sslmode", self.sslmode)
			self.options = config.get("options", self.options)

		# On récupère les paramètres s'ils ont été spécifiés
		if db_name is not None : self.database = db_name 
		if db_server is not None: self.host = db_server
		if db_port is not None: self.port = db_port
		if db_user is not None: self.user = db_user
		if db_password is not None: self.password = db_password
		if sslmode is not None: self.sslmode = sslmode
		if options is not None: self.options = options

		# On vérifie que la config est complète
		if self.database is None: raise ValueError("Le nom de la base de données n'a pas été spécifié")
		if self.host is None: raise ValueError("L'adresse de la base de données n'a pas été spécifiée")
		if self.port is None: raise ValueError("Le port de la base de données n'a pas été spécifié")
		if self.user is None: raise ValueError("L'utilisateur de la base de données n'a pas été spécifié")
		if self.password is None: self.passwd = ""
		if self.sslmode is None: raise ValueError("Le mode de ssl de la base de données n'a pas été spécifié")
		if self.options is None: raise ValueError("Les options de la base de données n'ont pas été spécifiées")

		# On crée les objets nécessaires pour plus tard
		self.db = None
		self.cursor = None

	def connect(self):
		""" Méthode pour se connecter à la base de données
			On commence par pinguer la db
		"""
		if "Request timed out" in ping(self.host, count=1):
			return False
		# Si le ping est passé on essaie de se connecter à la db
		if self.db_type == "postgresql":
			self.db = psycopg2.connect(host = self.host, port = self.port, database = self.database, user = self.user, password = self.password, sslmode = self.sslmode, options = self.options)
		elif self.db_type == "mariadb":
			self.db = mariadb.connect(host = self.host, port = self.port, database = self.database, user = self.user, password = self.password)
		elif self.db_type == "mysql":
			self.db = mysql.connector.connect(host = self.host, database = self.database, user = self.user, password = self.password)
		elif self.db_type == "sqlserver":
			# Le premier driver trouvé sera utilisé
			driver = pyodbc.drivers()[0]
			self.db = pyodbc.connect("DRIVER={" + driver + "};SERVER=" + self.host + "," + self.port + ";DATABASE=" + self.database + ";UID=" + self.user \
				+ ";PWD=" + self.password + ";TrustServerCertificate=YES;" )

		if self.db is None:
			return False
		else:
			return True

	def disconnect(self):
		""" Méthode pour déconnecter la db """
		self.db.close()

	def open(self, auto_connect=True, fetch_type='tuple'):
		""" Méthode pour créer un curseur """
		if auto_connect:
			if not self.connect(): return False
		# On essaye de fermer le curseur avant d'en recréer un pour si il existe déjà
		try:
			self.cursor.close()
		except:
			pass

		# Si fetch_type incorrect
		if fetch_type not in ('tuple', 'list', 'dict', 'dict_name'): raise ValueError("Incorrect fetch_type")
		# Si postgresql on spécifie un paramètre pour récupérer les titres des colonnes
		if self.db_type == 'postgresql' and fetch_type in ('dict', 'dict_name'):
			self.cursor = self.db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
		else:
			self.cursor = self.db.cursor()
		# Résultat de la création du curseur
		if self.cursor is not None:
			return True
		else:
			return False

	def commit(self):
		""" Méthode qui met à jour la db """
		self.db.commit()

	def close(self, commit = False, auto_connect=True):
		""" Méthode pour détruire le curseur, avec ou sans commit """
		# Si commit demandé à la fermeture
		if commit:
			self.db.commit()
		self.cursor.close()
		if auto_connect:
			self.disconnect()
		
	def execute(self, query, params = None):
		""" Méthode pour exécuter une requête mais qui gère les drop de curseurs """
		self.cursor.execute(query, params)

	def exec(self, query, params = None, fetch = "all", auto_connect=True, fetch_type='tuple'):
		""" Méthode pour exécuter une requête et qui ouvre et ferme  la db automatiquement """
		# Détermination du commit
		if (not "SELECT" in query.upper()[:20]) and (not "SHOW" in query.upper()[:20]) and (not "RETURNING" in query.upper()):
			commit = True
		else:
			commit = False
		# Ouverture de l'accès à la db
		if self.open(auto_connect=auto_connect, fetch_type=fetch_type):
			self.execute(query, params)
			# Si pas de commit ce sera une récupération
			if not commit:	
				# S'il faut récupérer les titres
				if fetch_type == "dict_name":
					fetch_title = True
				else:
					fetch_title = False
				# Type de récupération des données
				if fetch == "all":
					value = self.fetchall()
					if fetch_title:
						value = self.extract_title(value, fetch)
				elif fetch == "one":
					value = self.fetchone()
					if fetch_title:
						value = self.extract_title(value, fetch)
					# On vide le curseur pour éviter l'erreur de data restantes à la fermeture
					trash = self.fetchall()
				elif fetch == "single":
					value = self.fetchone()
					if fetch_title:
						value = self.extract_title(value, fetch)
					elif value is not None:
						value = value[0]
					# On vide le curseur pour éviter l'erreur de data restantes à la fermeture
					trash = self.fetchall()
				elif fetch == 'list':
					# On renvoie une liste composée du premier élément de chaque ligne
					value = [item[0] for item in self.fetchall()]
				else:
					raise ValueError("Wrong fetch type")
				self.close(auto_connect=auto_connect, commit=commit)
				# Si fetch_type == 'list' on transforme le tuple en liste
				if fetch_type == "list":
					if fetch == "all":
						value = [list(item) for item in value]
					elif fetch in ("one", "single"):
						value = list(value)
				return value
			else:
				self.close(auto_connect=auto_connect, commit=commit)
		else:
			raise AttributeError("Erreur de création du curseur pour l'accès à la db")

	def fetchall(self):
		""" Méthode pour le fetchall """
		return self.cursor.fetchall()

	def fetchone(self):
		""" Méthode pour le fetchone """
		return self.cursor.fetchone()

	def replace_none_list(self, liste):
		""" Remplacer les None contenus dans la liste par une string vide """
		# On regarde si c'est une liste à un ou deux niveaux
		level = 1
		if type(liste[0]) == list:
			level = 2

		for seq, item in enumerate(liste):
			if level == 1:
				if item == None:
					liste[seq] = ""
			elif level == 2:
				for sub_seq, sub_item in enumerate(item):
					if sub_item == None:
						liste[seq][sub_seq] = ""

		return liste

	def extract_title(self, value, fetch):
		""" On extrait les titres du résultat et on renvoie le bon type de donnée en fonction du fetch 
			on renvoie une liste dont le premier élément sera une liste avec les titres des colonnes
			et le 2e élément sera une liste avec les données

			postgresql renvoie une liste de dictionnaires
		"""
		
		# Si aucune donnée, on renvoie une liste vide
		if value == []: return []

		if self.db_type == 'postgresql':
			if fetch == 'all':
				result = [value[0].keys(), self.replace_none_list([list(row.values()) for row in value])]
			elif fetch == "one":
				result = [value.keys(), self.replace_none_list(list(value.values()))]
			elif fetch == "single":
				result = [list(value.keys())[0], self.replace_none_list(list(value.values()))[0]]
		
		elif self.db_type in ('mysql', 'mariadb'):
			result = [[i[0] for i in self.cursor.description], self.replace_none_list([row for row in value])]

		return result

	def __enter__(self):
		""" Ouverture avec with """
		self.connect()
		return self

	def __exit__(self, *args, **kwargs):
		""" Fermeture avec with """
		self.disconnect()

	def handle_datetimeoffset(dto_value):
	    # ref: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
	    tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 0, -6, 0)
	    tweaked = [tup[i] // 100 if i == 6 else tup[i] for i in range(len(tup))]
	    return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:07d} {:+03d}:{:02d}".format(*tweaked)