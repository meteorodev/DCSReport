"""
version: 1.0, date: 2024-10-23
Class CreateTables
This class implements functions to create tables in PostgreSQL database if they don't exist
developer by ...
Copyright. INAMHI @ 2024 <www.inamhi.gob.ec>. all rights reserved.
"""
import psycopg2
from utils.manage_conf import get_cred

class CreateTables:
    def __init__(self):
        self.params = get_cred(section="postgresql2")

    def crear_tabla_si_no_existe(self, nombre_tabla):
        """
        Crea una tabla si no existe con la estructura requerida
        Args:
            nombre_tabla (str): Nombre de la tabla a crear sin el prefijo '_'
        Returns:
            bool: True si la tabla fue creada o ya existía, False si hubo error
        """
        conn = None
        try:
            conn = psycopg2.connect(**self.params)
            cur = conn.cursor()

            # Verificar si la tabla existe
            cur.execute(f"""
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.tables 
                    WHERE table_schema = 'automaticas' 
                    AND table_name = '_{nombre_tabla}'
                );
            """)
            exists = cur.fetchone()[0]

            if not exists:
                # Crear la tabla si no existe
                create_table_query = f"""
                    CREATE TABLE IF NOT EXISTS automaticas._{nombre_tabla} (
                        id_usuario integer,
                        id_estacion integer,
                        estacion text COLLATE pg_catalog."default",
                        fecha_toma_dato timestamp without time zone,
                        fecha_ingreso timestamp without time zone,
                        valor double precision,
                        instrumento integer,
                        nivel_calidad integer,
                        estado_calidad integer,
                        frecuencia_trans integer
                    );
        """
                cur.execute(create_table_query)
                
                # Crear índices para optimizar las consultas
                cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{nombre_tabla}_id_estacion 
                ON automaticas._{nombre_tabla} (id_estacion);
                """)
                
                cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{nombre_tabla}_fecha 
                ON automaticas._{nombre_tabla} (fecha_toma_dato);
                """)

                conn.commit()
                print(f"Tabla _{nombre_tabla} creada exitosamente")
            else:
                print(f"La tabla _{nombre_tabla} ya existe")

            return True

        except (Exception, psycopg2.Error) as error:
            print(f"Error al crear la tabla _{nombre_tabla}: {error}")
            if conn:
                conn.rollback()
            return False
            
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()