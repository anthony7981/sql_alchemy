#En primer lugar, importamos las herramientas necesarias para trabajar
import datetime
import logging

import pandas as pd
import numpy as np
from sqlalchemy import MetaData, Table, Column
from sqlalchemy import String, Integer
from sqlalchemy import create_engine
import psycopg2
import requests
from decouple import config
"""
Guardamos en este comentario las columnas requeridas para guardar toda
la información en un mismo dataframe (solo a modo de anotación):

    [
        "cod_localidad",           "id_provincia",      "id_departamento",
        "categoría",               "provincia",         "localidad",
        "nombre",                  "domicilio",         "código postal",
        "número de teléfono",      "mail",              "web"
    ]

"""

# Pasamos el día actual como una variable para la creación de archivos.
# Obtenemos las urls de descarga y guardamos los documentos respectivamente.

logging.info("Descargando archivos")
logging.warning("Los archivos serán sobreescritos en caso de existir")


today = datetime.date.today()



museums_url = requests.get('https://docs.google.com/spreadsheets/d/1PS2_yAvNVEuSY0gI8Nky73TQMcx_G1i18lm--jOGfAA/export?format=csv&gid=514147473')
with open(f"museos/2022-septiembre/museos-{today.day}-{today.month}-{today.year}.csv",
        'wb') as file:
    file.write(museums_url.content)


cinemas_url = requests.get("https://docs.google.com/spreadsheets/d/1o8QeMOKWm4VeZ9VecgnL8BWaOlX5kdCDkXoAph37sQM/export?format=csv&gid=1691373423")
with open(f"cine/2022-septiembre/cine-{today.day}-{today.month}-{today.year}.csv",
    'wb') as file:
    file.write(cinemas_url.content)


libraries_url = requests.get("https://docs.google.com/spreadsheets/d/1udwn61l_FZsFsEuU8CMVkvU2SpwPW3Krt1OML3cYMYk/export?format=csv&gid=1605800889")
with open(f"bibliotecas/2022-septiembre/bibliotecas-{today.day}-{today.month}-{today.year}.csv",
    'wb') as file:
    file.write(libraries_url.content)
    


logging.info("Procesando los datos...")


"""

Ahora obtenemos los dataframes de los csv creados, normalizamos
la información del teléfono (que está compuesta por el código
de área y el número) y llenamos la información falante en las categorías.

Posteriormente, tratamos de almacenar en una variable 'categoría_main'
los datos que necesitamos de la variable de cada categoría. Entonces
llenamos los campos vacíos y renombramos las columnas para darle homogeneidad
a todas las tablas.

Por último, Si alguna o varias columnas no fueron encontradas (KeyError), 
establecemos los nombres de todas las columnas con los datos del csv,
para luego hacer la conversión.

"""

################## MUSEOS ######################

museums = pd.read_csv(f"museos/2022-septiembre/museos-{today.day}-{today.month}-{today.year}.csv")
museums['telefono'] = str(museums.cod_area) + str(museums.telefono)
museums['subcategoria'].fillna('Museos', inplace=True)

try:
    museums_main = museums[
        [
            'Cod_Loc',          'IdProvincia',       'IdDepartamento',
            'subcategoria',     'provincia',         'localidad',
            'nombre',           'direccion',         'CP',
            'telefono',         'Mail',              'Web'
        ]
    ].fillna('N/A').rename(columns={
        'Cod_Loc':              'cod_localidad',
        'IdProvincia':          'id_provincia',
        'IdDepartamento':       'id_departamento',
        'subcategoria':         'categoría',
        'direccion':            'domicilio',
        'cod_area':             'código postal',
        'telefono':             'número de teléfono',
        'Mail':                 'mail',
        'Web':                  'web'
        }
    )

except KeyError:
    museums.columns = [
        'Cod_Loc',              'IdProvincia',         'IdDepartamento',
        'Observaciones',        'categoria',           'subcategoria',
        'provincia',            'provincia',           'nombre',
        'direccion',            'piso',                'CP',
        'cod_area',             'telefono',            'Mail',
        'Web',                  'Latitud',             'Longitud',
        'TipoLatitudLongitud',  'Info_adicional',      'fuente',
        'jurisdiccion',         'año_inauguracion',    'actualizacion'
    ]
    museums_main = museums[
        [
            'Cod_Loc',          'IdProvincia',       'IdDepartamento',
            'subcategoria',     'provincia',         'localidad',
            'nombre',           'direccion',         'CP',
            'telefono',         'Mail',              'Web'
        ]
    ].fillna('N/A').rename(columns={
        'Cod_Loc':           'cod_localidad',
        'IdProvincia':       'id_provincia',
        'IdDepartamento':    'id_departamento',
        'subcategoria':      'categoría',
        'direccion':         'domicilio',
        'cod_area':          'código postal',
        'telefono':          'número de teléfono',
        'Mail':              'mail',
        'Web':               'web'
        }
    )




################# CINES #################

cinemas = pd.read_csv(f"cine/2022-septiembre/cine-{today.day}-{today.month}-{today.year}.csv")
cinemas['Teléfono'] = str(cinemas.cod_area) + str(cinemas.Teléfono)
cinemas['Categoría'].fillna("Salas de cine", inplace=True)

try:
    cinemas_main = cinemas[
        [
            'Cod_Loc',        'IdProvincia',     'IdDepartamento',
            'Categoría',      'Provincia',       'Localidad',
            'Nombre',         'Dirección',       'CP',
            'Teléfono',       'Mail',            'Web'
        ]
    ].fillna('N/A').rename(columns={
        'Cod_Loc':            'cod_localidad',
        'IdProvincia':        'id_provincia',
        'IdDepartamento':     'id_departamento',
        'Categoría':          'categoría',
        'Provincia':          'provincia',
        'Localidad':          'localidad',
        'Nombre':             'nombre',
        'Dirección':          'domicilio',
        'CP':                 'código postal',
        'Teléfono':           'número de teléfono',
        'Mail':               'mail',
        'Web':                'web'
        }
    )

except KeyError:
    cinemas.columns = [
        'Cod_Loc',                  'IdProvincia',                'IdDepartamento',           
        'Observaciones',            'Categoría',                  'Provincia',
        'Departamento',             'Localidad',                  'Nombre',                   
        'Dirección',                'Piso',                       'CP',
        'cod_area',                 'Teléfono',                   'Mail',
        'Web',                      'Información adicional',      'Latitud',                  
        'Longitud',                 'TipoLatitudLongitud',        'Fuente',                   
        'tipo_gestion',             'Pantallas',                  'Butacas',           
        'espacio_INCAA',            'año_actualizacion'
    ]

    cinemas_main = cinemas[
        [
            'Cod_Loc',        'IdProvincia',     'IdDepartamento',
            'Categoría',      'Provincia',       'Localidad',
            'Nombre',         'Dirección',       'CP',
            'Teléfono',       'Mail',            'Web'
        ]
    ].fillna('N/A').rename(columns={
        'Cod_Loc':              'cod_localidad',
        'IdProvincia':          'id_provincia',
        'IdDepartamento':       'id_departamento',
        'Categoría':            'categoría',
        'Provincia':            'provincia',
        'Localidad':            'localidad',
        'Nombre':               'nombre',
        'Dirección':            'domicilio',
        'CP':                   'código postal',
        'Teléfono':             'número de teléfono',
        'Mail':                 'mail',
        'Web':                  'web'
        }
    )



################### LIBRERÍAS #########################

libraries = pd.read_csv(f"bibliotecas/2022-septiembre/bibliotecas-{today.day}-{today.month}-{today.year}.csv")
libraries['Teléfono'] = str(libraries.Cod_tel) + str(libraries.Teléfono)
libraries['Categoría'].fillna("Bibliotecas Populares", inplace=True)

try:
    libraries_main = libraries[
        [
            'Cod_Loc',	     'IdProvincia',    'IdDepartamento',	
            'Categoría',     'Provincia',	   'Localidad',	
            'Nombre', 	     'Domicilio',	   'CP',	
            'Teléfono',	     'Mail',	       'Web'	
        ]
    ].fillna('N/A').rename(columns={
        'Cod_Loc':           'cod_localidad',
        'IdProvincia':       'id_provincia',
        'IdDepartamento':    'id_departamento',
        'Categoría':         'categoría',
        'Provincia':         'provincia',
        'Localidad':         'localidad',
        'Nombre':            'nombre',
        'Domicilio':         'domicilio',
        'CP':                'código postal',
        'Teléfono':          'número de teléfono',
        'Mail':              'mail',
        'Web':               'web'
    }
    )

except KeyError:
    libraries.columns = [
        'Cod_Loc',	        'IdProvincia',	      'IdDepartamento',
        'Observacion',	    'Categoría',	      'Subcategoria',
        'Provincia',	    'Departamento',	      'Localidad',	
        'Nombre'	        'Domicilio',	      'Piso'
        'CP',	            'Cod_tel',	          'Teléfono',
        'Mail',	            'Web',	              'Información adicional',
        'Latitud',	        'Longitud',	          'TipoLatitudLongitud',
        'Fuente',	        'Tipo_gestion',	      'año_inicio',
        'Año_actualizacion'
    ]

    libraries_main = libraries[
        [
            'Cod_Loc',	     'IdProvincia',    'IdDepartamento',	
            'Categoría',     'Provincia',	   'Localidad',	
            'Nombre', 	     'Domicilio',	   'CP',	
            'Teléfono',	     'Mail',	       'Web'	
        ]
    ].fillna('N/A').rename(columns={
        'Cod_Loc':              'cod_localidad',
        'IdProvincia':          'id_provincia',
        'IdDepartamento':       'id_departamento',
        'Categoría':            'categoría',
        'Provincia':            'provincia',
        'Localidad':            'localidad',
        'Nombre':               'nombre',
        'Domicilio':            'domicilio',
        'CP':                   'código postal',
        'Teléfono':             'número de teléfono',
        'Mail':                 'mail',
        'Web':                  'web'
    }
    )



# En la variable 'main' concatenamos los 3 dataframes.

main = pd.concat([museums_main, cinemas_main, libraries_main])
main.index = np.arange(1, len(main)+1)



# Construyendo los dataframes restantes ordenados por provincia.

################# REGISTRO DE CINES ###################

# Creamos el dataframe y establecemos 3 listas vacías para
# posteriormente iterar los 'groupby' con 'iteritems'.

reg_cinema = pd.DataFrame(columns=['Provincia', 'Pantallas', 'Butacas', 'espacio_INCAA'])

provincias = []
pantallas = []
butacas = []
incaa = []

grouped_provincias= cinemas['Provincia'].groupby(cinemas['Provincia'], as_index=True).unique()
grouped_pantallas = cinemas['Pantallas'].groupby(cinemas['Provincia'], as_index=True).sum()
grouped_butacas = cinemas['Butacas'].groupby(cinemas['Provincia'], as_index=True).sum()
grouped_incaa = cinemas['espacio_INCAA'].groupby(cinemas['Provincia'], as_index=True).count()

for x in grouped_provincias.iteritems():
    provincias.append(x[0])

for x in grouped_pantallas.iteritems():
    pantallas.append(x[1])

for x in grouped_butacas.iteritems():
    butacas.append(x[1])

for x in grouped_incaa.iteritems():
    incaa.append(x[1])

reg_cinema['Provincia'] = np.array(provincias, dtype=str)
reg_cinema['Pantallas'] = np.array(pantallas, dtype=np.int32)
reg_cinema['Butacas'] = np.array(butacas, dtype=np.int32)
reg_cinema['espacio_INCAA'] = np.array(incaa, dtype=np.int32)
print(reg_cinema)



###################### REGISTRO POR CATEGORÍA ############################################
###################### ESTA TABLA SE ENCUENTRA PENDIENTE POR TERMINAR ####################

# En este caso, creamos el dataframe, pero necesitaremos añadirle
# las columnas respectivas a las fuentes y a las provincias.
# 
# FALTANTES: las fuentes y provincias

reg_category = pd.DataFrame(columns=['categoría', 'reg_cat'])

reg_c = []
reg_f = []
reg_p = []


reg_category['categoría'] = main['categoría'].unique()
grouped_cat = main['categoría'].groupby(main['categoría'], as_index=True).count()

for x in grouped_cat.iteritems():
    reg_c.append(x[1])

reg_category['reg_cat'] = np.array(reg_c, dtype=np.int32)


    






# Ahora comenzamos a trabajar con sqlalchemy.
# Pasamos con 'config' las variables de entorno del archivo '.env' para crear el motor.
# Luego creamos la tabla que almacenará a 'main' y las dos otras tablas requeridas.

logging.info("Interactuando con la base de datos")

engine = create_engine(f"postgresql://{config('POSTGRESQL_USER')}:{config('POSTGRESQL_PASSWORD')}@{config('POSTGRESQL_HOST')}:{config('POSTGRESQL_PORT')}/{config('POSTGRESQL_DB')}")

metadata_obj = MetaData()
main_table = Table(
        'main',
        metadata_obj,
        Column('id', Integer, primary_key=True),
        Column('cod_localidad', Integer),
        Column('id_provincia', Integer),
        Column('id_departamento', Integer),
        Column('categoría', String(40)),
        Column('provincia', String(40)),
        Column('localidad', String(50)),
        Column('nombre', String(100)),
        Column('domicilio', String(255)),
        Column('código postal', String(10)),
        Column('número de teléfono', Integer),
        Column('mail', String(100)),
        Column('web', String(100))
    )

reg_category_table = Table(
    'reg_category',
    metadata_obj,
    Column('categoría', String(40)),
    Column('regs_categoría', Integer),

)

reg_cinema_table = Table(
    'reg_cinema',
    metadata_obj,
    Column('Provincia', String(40)),
    Column('Pantallas', Integer),
    Column('Butacas', Integer),
    Column('espacio_INCAA', Integer)
)

with engine.begin() as conn:
    metadata_obj.create_all(conn)

logging.info("Tablas creadas")


main.to_sql(name="main", con=engine.connect(), if_exists='replace')
reg_cinema.to_sql(name='reg_cinema', con=engine.connect(), if_exists='replace')

logging.info("Datos ingresados")