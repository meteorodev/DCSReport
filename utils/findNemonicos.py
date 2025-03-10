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
        #self.mainpath = "/home/darwin/Documentos/Desarrollo/Python/DCSReport"
        self.mainpath = "/media/Datos/Desarrollo/PythonProyects/DCSReport/"

    def getListestation(self):
        print("inicio")
        meteoFile = self.mainpath+"/METEOS.nl"
        meteos = pd.read_csv(meteoFile, sep = ":", header=None)
        meteos = meteos.iloc[:,0:8]
        hidroFile = self.mainpath+"/HidroAlertas.nl"
        hidros = pd.read_csv(hidroFile, sep = ":", header=None)
        meteos = pd.concat([meteos,hidros], ignore_index=True)
        return meteos.sort_values(by=2)


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
        #directorio = '/home/darwin/Documentos/Desarrollo/Python/DCSReport'
        directorio = '/media/Datos/Desarrollo/PythonProyects/DCSReport/'
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
        return nemonicos

    def getstfromDB(self, stationlist):
        query=f"""
            SELECT p.codigo, p.nombre, e.id_estacion, c.latitud, c.longitud, c.altutid, cp.nombre
            FROM administrativo.puntos_observacion p, administrativo.estaciones e,
            administrativo.captores_tipos cpt,	administrativo.captores cp,
            administrativo.coordenadas c
            where e.id_captor_tipo = cpt.id_captor_tipo and e.id_punto_obs = p.id_punto_obs 
            and cpt.id_captor = cp.id_captor and e.id_punto_obs = c.id_punto_obs
            and  p.codigo in {stationlist} and cp.nombre = 'ELECTROMECANICA' order by p.codigo;
        """
        print("getstFromDB")
        conn = None
        try:
            params = conf.get_cred(fileconf='../config.ini',section="bandamhHist")
            print('Connecting to the PostgreSQL database bandamhHist...')
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute(query)
            colnames = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            if rows:
                df = pd.DataFrame(rows, columns=colnames)
                return df

            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')

    def rtTables(self):
        """ return dataframe with all tablets of realtime db to compare with nemonicos
        Args:
            no args
        Raises:
            RuntimeError:
        Returns:
            dataframe : list tables with nemonicos names
        """
        query = f"""
                    SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE';
                """
        conn = None
        try:
            params = conf.get_cred(fileconf='../config.ini', section="bandahmtime")
            print('Connecting to the PostgreSQL database bandamhreal time..')
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute(query)
            colnames = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            if rows:
                df = pd.DataFrame(rows, columns=colnames)
                return df

            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')


    def compareIdSt(self):
        st = self.getListestation()

        st_id = tuple(st.iloc[:,2])
        stdb = self.getstfromDB(st_id)
        print("inicio")
        #st.to_csv("./Estaciones.csv",index=False)
        df1 = st.iloc[:,[2,1]]
        df2 = stdb.iloc[:,[0,2]]
        df1.columns = df2.columns
        #verificamos si existen todas las estaciones
        are_iq = set(df1['codigo'])==set(df2['codigo'])
        #### check id_estacion
        margedf=pd.merge(df1,df2,on='codigo',suffixes=('_nl','_db'))
        dif = margedf[margedf['id_estacion_nl'] != margedf['id_estacion_db']]
        return margedf

    def compareNemonicos(self):
        nemo = nm.getListNemonicos()
        tablas = nm.rtTables()
        # limpiamos el dataframe de nemonicos quitando lso repetidos
        cl_nemos = nemo.drop_duplicates()
        cl_nemos = cl_nemos.dropna()
        ####
        print("control")
        # Filtro usando regex
        cl_tablas = tablas[tablas['table_name'].str.match(r"^_\d{9}[A-Za-z]$", na=False)]
        cl_tablas = cl_tablas['table_name'].str.lstrip('_')
        cl_nemos.columns = ["table_name"]
        # Filtrar los valores que están en s1 pero NO en s2
        s_diff = cl_nemos[~cl_nemos.isin(cl_tablas)]
        ####  tablas faltantes
        # 01807011m, 007060101h, 013130101h, 017140805m, 022203201h, 017140805h
        return s_diff

if __name__ == '__main__':
    nm = NemonicoChecker()

    #nm.test_postgres_conn(section='postgresql2')
    #nm.getListestation()
    nm.compareIdSt()
    # nemo = nm.getListNemonicos()
    # tablas = nm.rtTables()
    nm.compareNemonicos()