"""
version: 1.0, date: 24/1/25
Class  This class check if nemonicosexist into real time database, takes nemonico2 colum from all var_group file to test.

developer by darwin11rv@gmail.com
Copyright. INAMHI @ 2025 <www.inamhi.gob.ec>. all rights reserved.
"""

import utils.manage_conf as conf
import psycopg2
import os
import pandas as pd

class NemonicoChecker():
    
    def __init__(self):
        pass

    def getListNemonicos(self):
        """ This function make a list of nemonicos base on var_group files, open each file a get the last column
            Args:
                 ():
            Raises:
                :
            Returns:
                :
        """
        # Ruta del directorio donde se encuentran los archivos
        directorio = '/home/darwin/Documentos/Desarrollo/Python/DCSReport'
        # Lista para almacenar los DataFrames
        dataframes = []
        # Iterar sobre los archivos en el directorio
        for archivo in os.listdir(directorio):
            # Verificar si el archivo comienza con 'var_grupo' y termina con '.csv'
            if archivo.startswith('var_grupo') and archivo.endswith('.csv'):
                # Construir la ruta completa del archivo
                ruta_completa = os.path.join(directorio, archivo)
                # Leer el archivo CSV y crear un DataFrame
                df = pd.read_csv(ruta_completa,sep= ';')
                # Agregar el DataFrame a la lista
                dataframes.append(df)
        df_combinado = pd.concat(dataframes, ignore_index=True)
        nemonicos = df_combinado['nemonico2']
        # Mostrar el DataFrame combinado (opcional)
        print(df_combinado)


    def test_postgres_conn(self, section='postgresql'):
        conn = None
        try:
            params = conf.get_cred(section=section)
            print('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            print('PostgreSQL database version:')
            cur.execute('select version(); ')
            db_version = cur.fetchone()
            print(db_version)
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')


if __name__ == '__main__':
    nm = NemonicoChecker()

    #nm.test_postgres_conn(section='postgresql2')
    nm.getListNemonicos()
