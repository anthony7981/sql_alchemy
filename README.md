# README

En este archivo encontrarás la manera de crear tu entorno virtual, instalar las dependencias y configurar las variables de entorno.

Debes dirigirte a la carpeta donde guardarás el repositorio y, en primer lugar, crea tu entorno con virtualenv del siguiente modo:

	virtualenv -p python3 setup
	
Acto seguido, instala todas las dependencias con el siguiente comando:

	pip install -r requirements.txt
	
Hecho eso, solo queda crear un archivo .env que deberá contener la siguiente información:

	POSTGRESQL_HOST=(tu host)
	POSTGRESQL_USER=(tu usuario)
	POSTGRESQL_PASSWORD=(tu contraseña)
	POSTGRESQL_DB=(tu base de datos)
	POSTGRESQL_PORT=(tu puerto)
	
