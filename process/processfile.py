"""
version: 1.0, date: 2023-05-20
Class ProcessDownloadFile
This class implements functions to process excel file extracted from CDS platform en send this information into database
developer by darwin11rv@gmail.com
Copyright. INAMHI @ 2023 <www.inamhi.gob.ec>. all rights reserved.
"""
from datetime import datetime, timedelta
import pymongo
import psycopg2
import pandas as pd
from process.makeBkFile import MakeBackupFile
import utils.manage_conf as conf
from utils.decoder_met import Msg_Met_Decoder
import os
import numpy as np


class ProcessDownloadFile(object):

    def __init__(self):
        self.makebk = MakeBackupFile()

    @staticmethod
    def test_mongo_connection(section="mongodb"):
        """ This function test connection with mongo database
            Args:
                section (str): section from config file by default mongodb
            Raises:
                server Timeout: time out error
            Returns:
                NONE:
        """
        client = None
        try:

            params = conf.get_cred(section=section)
            cad = "mongodb://" + params['user'] + ":" + params['password'] + "@" + \
                  params['host'] + ":27017/" + params['database']
            # print(cad)
            client = pymongo.MongoClient(cad)
            db = client[params['database']]
            print(db.list_collection_names())
            print("***********************************")
            print("***********************************")
            print(params['database'], " : ", params['collection'])
            collection = db[params['collection']]
            datos = list(collection.find().limit(1))
            print(datos[0])
        except pymongo.errors.ServerSelectionTimeoutError as error:
            print('Error with MongoDB connection: %s' % error)
        except pymongo.errors.ConnectionFailure as error:
            print('Could not connect to MongoDB: %s' % error)
        finally:
            if client is not None:
                client.close()
                print('Database Mongo connection closed.')

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

    def get_stations_file(self, fileST):
        """ Return a list of stations with INAMHI code an NESDIS code to compare with reports
        Args:
            fileST (str): file with a list station in .nl format
        Returns:
            stations (dataframe) : dataframe with stations.
        """
        # TODO en este caso solo se llera un archivo de codigos, se debe implemetar una tabla de la cual
        #  se extraidga esta inf
        stations = pd.read_csv(fileST, sep=":", header=None)
        return stations

    def get_config_sta(self, id_group):
        """ This function load from disk var_groupX.csv
            Args:
                id_group (int): group identify read from .nl file
            Raises:
                : No Raises implement
            Returns:
                data_frame or None: A Datafreme whit file information or None if file not exist
        """
        group = "./var_grupo" + str(id_group) + ".csv"
        if os.path.exists(group):
            var_order = pd.read_csv(group, sep=";")
            # print(var_order)
            return var_order
        else:
            return None


    def set_station_state(self, id_station, new_state):
        """ Cambia el estado de una estacion Transmite , No transmite, transmite con errores
        Args:
            id_station (int): Identificador de la estacion
            new_state (int) : Nuevo estado de transmisión de la estacion
        Raises:
            RuntimeError: Rrror de conexion a postgreSQL
        """

        conn = None
        try:
            # read connection parameters
            params = conf.get_cred(section="postgresql")
            conn = psycopg2.connect(**params)
            # create a cursor
            cur = conn.cursor()
            # aqui se cambia el estado de la estacion
            query = "select id_estado from administrative.estaciones_estados where id_estacion = " + str(
                id_station)
            cur.execute(query)
            est_actual = cur.fetchone()
            # print("no cambio el estado")
            if est_actual[0] != new_state:
                # print("Estado Actual ", est_actual[0], "Id_estacion ", id_estacion)
                query = "update administrative.estaciones_estados set id_estado = " + str(
                    new_state) + " where id_estacion = " + str(id_station) + ";"
                # print(query)
                cur.execute(query)
                conn.commit()
            # close the communication with the PostgreSQL
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                # print('Database connection closed.')

    def text_to_date(self, textdate):
        """ This function make a text to date compare two different formats to date
        Args:
            textdate (str): date in text format
        Raises:
            RuntimeError: no errors
        Returns:
            my_date : date object
        """
        if isinstance(textdate, datetime):
            return textdate
        try:
            if (len(textdate.split("-")) > 1):  # for date in  y-m-d, H:M:S 2023-05-21,00:00:02, format
                my_date = datetime.strptime(textdate, "%Y-%m-%d %H:%M:%S")
            else:  # form date in 20230521210000 YmdHMS  format
                my_date = datetime.strptime(textdate, "%Y%m%d%H%M%S")
            return my_date
        except:
            return None

    def exist_id_mongo(self, id_datos):
        """ check if exist the record pass like a param in database
        Args:
            id_datos (str): id of record in mongo database  collection data1h
        Raises:
            RuntimeError: Error connection
        Returns:
            id de registro or None (str|None) : id in the database or None
        """
        try:
            params = conf.get_cred(section="mongodb")
            cad = "mongodb://" + params['user'] + ":" + params['password'] + "@" + \
                  params['host'] + ":27017/" + params['database']

            client = pymongo.MongoClient(cad, 1000)
            # print("obteniendo la base ")
            db = client[params['database']]
            # print(db.list_collection_names())
            #collection = db['data1h']
            collection = db[params['collection']]
            ### aqui se crea el diccionario para insertarlo en la base de datos
            ### primero comprobamos que cada valor sea nuemrico y si no lo es lo cambiamos por null
            datos = list(collection.find({"_id": id_datos}))
            if (len(datos) >= 1):
                return datos[0]["_id"]
            else:
                return None
        except pymongo.errors.ServerSelectionTimeoutError as error:
            print('Error with MongoDB connection: %s' % error)
            return 'Error with MongoDB connection: %s' % error
        except pymongo.errors.ConnectionFailure as error:
            print('Could not connect to MongoDB: %s' % error)
            return 'Could not connect to MongoDB: %s' % error

    def make_dic_insert(self, datos, fecha_dato, var_ord, cod_inamhi, pun_obs, qc_d=1 ):
        """ This function make a dictionary to insert datin into mongo db in data1h collection make
        data with a flag estado = -99 this mean that data was insert with the new process. teke this note form migration
        Args:
            datos (list): list of data with values from stations messages
            fecha_dato (date): Date of measure data
            var_ord (int): order in de message for this variable
            cod_inamhi (int): Id for the station in the database for example 3456
            pun_obs (str): Id for station in INAMHI format example M0001
            qc_d (int): if var_ord has data quality by default 1 this mean that each variable has QC see .nl file
        Raises:
            RuntimeError:
        Returns:
           data_insert (dic) : dictionary for insert in mongodb
        """
        data_dic = {}
        # firsts clean datos list to complete items like var_ord length
        len_var_ord= len(var_ord.iloc[:, 0])
        len_data = len(datos)
        #print(datos)
        if len_var_ord >= len_data:
            dif_len = len_var_ord + 1 - len_data
            for i in range(0,dif_len):
                datos.append(None)
        else: # when datos has more items than var_ord
            datos = datos[0:(len_var_ord+1)]
        # clean datos
        for i in range(1,len_var_ord):
            if datos[i] is not None:
                #print("datos[i]",datos[i])
                if str(datos[i]).replace(".", "").replace("-", "").replace("+", "").isnumeric() == False:
                    #print("iter", i, "no es Digito", datos[i], pun_obs)
                    datos[i] = None
        # print(".... ... ... .     ",qc_d)
        if qc_d == 1:
            for d in range(1, len(var_ord.iloc[:, 0]), 2):
                dic_nemo = {"sensor": "1", "valor": datos[d], "calidad": datos[d + 1]}
                data_dic[var_ord.iloc[(d - 1), 0]] = [dic_nemo]
        if qc_d == 0:
            for d in range(1, len(var_ord.iloc[:, 0]), 1):
                dic_nemo = {"sensor": "1", "valor": datos[d], "calidad": "55"}
                data_dic[var_ord.iloc[d, 0]] = [dic_nemo]

        #print(data_dic)
        id_punto = str(cod_inamhi) + "_D1_STGO_R_" + fecha_dato.strftime("%Y-%m-%d_%H:%M:%S")
        # print(id_punto, pun_obs)
        fecha_cre = datetime.now()
        data_insert = {"_id": id_punto, "puntoObservacion": pun_obs, "estacion": int(cod_inamhi),
                       "medioTransmision": "STGO",
                       "fechaCreacionArchivo": fecha_cre, "fechaTomaDato": fecha_dato, "estado": -99,
                       "data": data_dic}
        
        print(data_dic)
        #print(data_insert)

        return data_insert


    def verify_insert(self, data2insert, nesdis, cod_inamhi, pun_obs, collection):
        if data2insert["_id"] == self.exist_id_mongo(data2insert["_id"]):
            print(data2insert["_id"], "ya existe no se ingresa;", nesdis, ";id_est;", str(cod_inamhi),
                  ";cod_inamhi;", pun_obs)
            return 200
        else:
            x = collection.insert_one(data2insert)
            return "insertado " + str(x.inserted_id)

    def insert_data(self, datos, var_ord, nesdis, cod_inamhi, pun_obs, msg_type, date_file,qc_d = 1 ):
        """ This function make a dictionary to insert datin into mongo db in data1h collection make
            data with a flag estado = -99 this mean that data was insert with the new process. teke this note form migration
            Args:
                datos (list) or string : list of data with values from stations messages
                var_ord (dataframe): order in de message for this variable with cols(nemonico, variable, orden, decimales, caracteres)
                nesdis (str): id form noaa dcs system example 395685A
                cod_inamhi (int): Id for the station in the database for example 3456
                pun_obs (str): Id for station in INAMHI format example M0001
                msg_type (int): type message for process 0: for clear text message with one hour message
                                1: for psedubinary message with two messages in one
                                2: for clear text message with two message in ona
                date_file (series): date obtained from Excel file field "CAR TIME"
                qc_d (int): if var_ord has data quality by default 1 this mean that each variable has QC see .nl file
            Raises:
                RuntimeError: time out for database connection
            Returns:
                No returns.
        """
        try:
            params = conf.get_cred(section="mongodb")
            cad = "mongodb://" + params['user'] + ":" + params['password'] + "@" + \
                  params['host'] + ":27017/" + params['database']
            print("cadena de conexion se ve a continuacion ")
            #print(cad)
            #print(params['database'], " : ", params['collection'])

            client = pymongo.MongoClient(cad, 1000)
            # print("obteniendo la base ")
            db = client[params['database']]
            #print(db.list_collection_names())
            #collection = db['data1h']
            collection = db[params['collection']]
            #### insert into the database dependency of msg type
            # for message in clear text and just one message
            print("def insert_data: ", nesdis,":", cod_inamhi ,":",pun_obs,": msg_type ",msg_type )
            print(datos)
            if msg_type == 0:
                textdate = datos[0]
                fecha_dato = self.text_to_date(textdate)
                if fecha_dato is not None:
                    #print(datos)
                    data2insert = self.make_dic_insert(datos, fecha_dato, var_ord, cod_inamhi, pun_obs,qc_d)
                    val = self.verify_insert(data2insert, nesdis, cod_inamhi, pun_obs, collection)
                    # self.makebk.add_line(path_backup, cod_inamhi, nesdis, fecha_dato)
            elif msg_type == 1: # one encode message
                #print("****************************************************************")
                dec = Msg_Met_Decoder()
                decimals = var_ord.iloc[:, 3]
                curr_date = datetime.strptime(date_file.iloc[0], "%m/%d/%Y %H:%M:%S.%f")
                curr_date = curr_date.replace(minute=00)
                for i in range(0, len(datos)):
                    fecha_dato = curr_date - timedelta(hours=(len(datos)-1-i))
                    #print( "fechaa guardar ",fecha_dato, "i",i, )

                    array_decode = dec.decomMesage(datos[i], var_ord.iloc[:,3], var_ord.iloc[:,4])
                    # print(array_decode)

                    for j in range(0, len(array_decode)):
                        if decimals[j] > 0:
                            array_decode[j] = array_decode[j] / 10 ** decimals[j]
                        if j > len(decimals): #
                            break
                    array_decode = ["fecha"]+array_decode
                    #print(array_decode)
                    data2insert = self.make_dic_insert(array_decode, fecha_dato, var_ord, cod_inamhi, pun_obs, qc_d)
                    val = self.verify_insert(data2insert, nesdis, cod_inamhi, pun_obs, collection)
                #print("****************************************************************")
            elif msg_type == 2:  # form message that have two messages in one
                textdate = datos[0] + " " + datos[1]
                
                fe = datetime.strptime(textdate, "%Y-%m-%d %H:%M:%S") + timedelta(hours = 1)
                st_find = "55" + datetime.strftime(fe,"%Y-%m-%d")
                val_cut = 0
                for ind, val in enumerate(datos):
                    if val.startswith(st_find):
                        val_cut = ind
                        break
                fecha_dato = self.text_to_date(textdate)
                vacio = ["vacio"]
                if val_cut > 0:
                    seg_1 = datos[2:val_cut - 1]
                    seg_1.append('55')
                    seg_1 = vacio+seg_1
                    data2insert = self.make_dic_insert(seg_1, fecha_dato, var_ord, cod_inamhi, pun_obs, qc_d)
                    val = self.verify_insert(data2insert, nesdis, cod_inamhi, pun_obs, collection)
                    seg_2 = datos[val_cut + 2:]
                    seg_2 = vacio + seg_2
                    data2insert = self.make_dic_insert(seg_2, fe, var_ord, cod_inamhi, pun_obs, qc_d)
                    val = self.verify_insert(data2insert, nesdis, cod_inamhi, pun_obs, collection)
                else:
                    data2insert = self.make_dic_insert(datos, fecha_dato, var_ord, cod_inamhi, pun_obs, qc_d)
                    val = self.verify_insert(data2insert, nesdis, cod_inamhi, pun_obs, collection)

            else:
                print("msg_type undefined")
            #### ********************************

        except pymongo.errors.ServerSelectionTimeoutError as error:
            print('Error with MongoDB connection: %s' % error)
            return 'Error with MongoDB connection: %s' % error
        except pymongo.errors.ConnectionFailure as error:
            print('Could not connect to MongoDB: %s' % error)
            return 'Could not connect to MongoDB: %s' % error
        self.insert_data_postgres(datos, var_ord, self.stations, self.file_type, self.row_index, pun_obs, msg_type, date_file)

    # esta funcion le el excel descargado coteja la infromación con elcodigo INAMHI y lo ingresa a la base de datos
    def read_excel_data(self, file_data, file_config, path_backup):
        data = pd.read_excel(file_data, engine="openpyxl")
        # Cleaning excel replace extra characters and make Address to str
        data = data.iloc[:, [0, 8, 17]]
        data['ADDRESS'] = data['ADDRESS'].replace("'", "")
        data['ADDRESS'] = data['ADDRESS'].astype(str)
        self.stations = self.get_stations_file(file_config)
        self.file_type = "HidroAlertas" if "HidroAlertas" in file_config else "METEOS"
        
        # print(stations.head(4))
        max_group = self.stations.iloc[:, 4].max()
        min_group = self.stations.iloc[:, 4].min()
        ### iter into group stations
        # in each iter load group file configuration
        for it in range(min_group, max_group + 1):
            # print("--------- ",it," ---------------")
            var_order = self.get_config_sta(it)
            #print("read_excel_data: var order **** \n ", var_order)
            # check if file of groups exist,
            if var_order is None:
                print("no configuration file exist var_group"+str(it)+".csv ")
            else:
                stations_gr = self.stations[self.stations.iloc[:, 4] == it]
                # print("grupo")
                # print(len(stations_gr))
                for row_it in range(0, len(stations_gr.iloc[:, 1])):
                    self.row_index = row_it  # Inicializar self.row_index aquí
                    nesdis = stations_gr.iloc[row_it, 0]
                    cod_inamhi = stations_gr.iloc[row_it, 1]
                    pun_obs = stations_gr.iloc[row_it, 2]
                    fila = data[data['ADDRESS'] == nesdis]
                    transmission_state = 6
                    msg_type = int(stations_gr.iloc[row_it, 6])  ## if the mesage is in pseudobinari format
                    qc_d = int(stations_gr.iloc[row_it, 7])
                    if len(fila) > 0:  # if nesdis exist into excel file
                        datos = fila.iloc[0, 2]
                        date_file = fila['CAR TIME']
                        if msg_type == 0 or msg_type == 2:
                            datos = datos.replace('b', '')
                            datos = datos.replace(" ", "").replace('`', '').replace('"', '')
                            datos = datos.rsplit(',')
                        else: # form message type 1 psudobinary
                            datos = datos.split(' ')
                        error_time = datetime.strptime(fila.iloc[0, 1], "%m/%d/%Y %H:%M:%S.%f")  # Car Time
                        if datos[0].replace(".", "").replace("-", "").isnumeric() or datos[0].startswith('@'):
                            transmission_state = 5
                            print(f"Inserting in PostgreSQL: {date_file}")

                            self.insert_data(datos, var_order, nesdis, cod_inamhi, pun_obs, msg_type, date_file, qc_d)
                        else:
                            ## con fallas trasnmite pero el mensaje continen errores
                            transmission_state = 4
                            print("El mensaje tienen errores;", nesdis, ";id_est;", str(cod_inamhi), ";cod_inamhi;",
                                pun_obs, error_time)

                        # transforma el campo de la fecha a fecha
                    # ********
                    # ********
                    # Quitar este comentario
                    # self.set_station_state(id_station=cod_inamhi, new_state=transmission_state)
                # print(data.head(5))
                
    def get_id_estacion(self,stations_data, file_type, row_index):
        """
        Obtiene el id_estacion basado en el tipo de archivo y el índice de fila.
        """
        if file_type == "HidroAlertas":
            return stations_data.iloc[row_index, 1]
        elif file_type == "METEOS":
            return stations_data.iloc[row_index, 8]
        else:
            return None

    def check_existing_record(self,cur, tabla, fecha_toma_dato, id_estacion):
        """
        Verifica si ya existe un registro para la fecha y estación dadas.
        """
        query = f"""
        SELECT COUNT(*) FROM automaticas._{tabla}
        WHERE fecha_toma_dato = %s AND id_estacion = %s
        """
        cur.execute(query, (fecha_toma_dato, id_estacion))
        return cur.fetchone()[0] > 0

    def insert_data_postgres(self, datos, var_ord, stations_data, file_type, row_index, pun_obs, msg_type, date_file):
        try:
            params = conf.get_cred(section="postgresql2")
            conn = psycopg2.connect(**params)
            cur = conn.cursor()

            # Obtener id_estacion
            id_estacion = self.get_id_estacion(stations_data, file_type, row_index)
            if id_estacion is None:
                print(f"No se pudo obtener id_estacion para el índice {row_index} en {file_type}")
                return

            # Convertir id_estacion a int estándar de Python si es numpy.int64
            if isinstance(id_estacion, np.integer):
                id_estacion = int(id_estacion)

            # Obtener fecha_toma_dato
            if msg_type == 0:
                fecha_toma_dato = self.text_to_date(datos[0])
            elif msg_type == 1:
                fecha_toma_dato = self.text_to_date(date_file.iloc[0])
            else:
                fecha_toma_dato = self.text_to_date(datos[0] + " " + datos[1])
            
            if fecha_toma_dato is None:
                print(f"Error: No se pudo convertir la fecha {datos[0]}")

            fecha_ingreso = datetime.now()

            for index, row in var_ord.iterrows():
                if pd.notna(row['nemonico2']):
                    tabla = row['nemonico2']
                    orden = int(row['orden'])  # Convertir a int estándar
                    decimales = int(row['decimales'])  # Convertir a int estándar

                    # Verificar si la tabla existe
                    cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '_{tabla}')")
                    table_exists = cur.fetchone()[0]

                    if not table_exists:
                        print(f"La tabla _{tabla} no existe.")
                        continue

                    # Verificar si el registro ya existe
                    if self.check_existing_record(cur, tabla, fecha_toma_dato, id_estacion):
                        print(f"Registro ya existe en _{tabla} para fecha {fecha_toma_dato} y estación {id_estacion}")
                        continue

                    # Obtener y procesar el valor
                    if msg_type == 1:
                        dec = Msg_Met_Decoder()
                        valor = dec.decomMesage(datos[0], [decimales], [int(row['caracteres'])])[0]
                    else:
                        valor = datos[orden] if orden < len(datos) else None

                    if valor is not None:
                        if isinstance(valor, np.generic):
                            valor = valor.item()  # Convertir tipos numpy a tipos Python estándar
                        if str(valor).replace(".", "").replace("-", "").replace("+", "").isnumeric():
                            valor = float(valor) / (10 ** decimales)
                        else:
                            valor = None
                    print(f"Inserting in PostgreSQL: {fecha_toma_dato}")

                    # Insertar el dato
                    query = f"""
                    INSERT INTO automaticas._{tabla} (fecha_toma_dato, fecha_ingreso, "1h", id_estacion, id_usuario, id_estado_proceso)
                    VALUES (%s, %s, %s, %s, 0, 19)
                    """
                    cur.execute(query, (fecha_toma_dato, fecha_ingreso, valor, id_estacion))

            conn.commit()
            print(f"Datos insertados correctamente para la estación {id_estacion}")

        except (Exception, psycopg2.Error) as error:
            print(f"Error al insertar datos en PostgreSQL: {error}")
            if conn:
                conn.rollback()
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()


#
#
if __name__ == '__main__':
    proc = ProcessDownloadFile()
    #     proc.readExcel("/home/darwin/Documentos/hidro2022_10_06_15_09.xlsx" )
    #proc.test_mongo_connection()
    # print(proc.__doc__)
#    paramas = conf.get_cred()
    # proc.read_excel_data("/home/darwin/Descargas/MessagesExport (5).xlsx", "../HidroAlertas.nl","/")
