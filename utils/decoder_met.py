# this script decompile seudobinary segment from aws-seudobinary-
import timeit
from datetime import timedelta, datetime
from time import time
import pandas as pd

class Msg_Met_Decoder():

    def bynary2text(self, msg, group_file,nesdis, id_estacion,estacion,ftd):
        """ This function get pseudobinary message and make dataframe pandas
            Args:
                msg (str):
                group_file (Dataframe): Pandas Dataframe with configuration
                nedis (str) : NESDIS code for station
                id_estacion (int) : identifier in database for station
                estacion (str) : INAMHI code for station
                ftd (date) : date to observation.
            Raises:
                No raises yet :
            Returns:
                Dataframe :
        """
        print("init decoder")
        decimals = group_file.iloc[:,3]
        nchar = group_file.iloc[:,4]
        nesdislist = group_file.iloc[:,5]
        dfdecode = pd.DataFrame(columns=["nesdis","id_usuario","id_estacion","estacion","fecha_toma_dato","fecha_ingreso",
                                         "valor","instrumento","nivel_calidad","estado_calidad","frecuencia_trans"])
        fid = datetime.now()
        pos = 0
        # @TODO aqui controlar que el segmentode mensaje sea de la misma posición 
        for idx,value in enumerate(nchar):
            seg = msg[pos:pos+value]
            # print( seg )
            datbin= ""
            if seg != "///":
                for s in seg:
                    dhex = format(ord(s), 'b')
                    datbin = datbin + dhex[-6:]
                    #print(hex, dhex, "datbin", datbin )
                datdec = int(datbin, 2) / 10 ** decimals[idx]
                print("SEG ",seg," dato ",datdec,  " car " ,nchar[idx], " dec " ,decimals[idx])
            #counter += i
            newline  = pd.DataFrame({
                "nesdis":nesdislist[idx],"id_usuario":0, "id_estacion":id_estacion, "estacion":estacion, "fecha_toma_dato":ftd,
                "fecha_ingreso":fid,"valor":datdec, "instrumento":None, "nivel_calidad":0, "estado_calidad":4, "frecuencia_trans":4
            },index=[0])
            dfdecode = pd.concat([dfdecode, newline], ignore_index = True)
            pos += value
        print("fin")
        return dfdecode
        # "id_usuario","id_estacion","estacion","fecha_toma_dato"     ,"fecha_ingreso"             , "valor","instrumento","nivel_calidad","estado_calidad","frecuencia_trans"
        # 0           ,63781        ,"M5190",   "2024-10-25 22:00:13" ,"2024-10-25 17:46:07.464358", "55.9"  ,NULL        ,     0        , 4             ,4


    def decoPseudobinario(cadena, caracteres,
                          multiplicador):
        """ This function get pseudobinary message and make dataframe pandas
            Args:
                cadena (str): This function get pseudobinary message and make dataframe pandas
                characters ([int]): Array with number of character by each parameter
                decimals ([int]) : Array with number of decimal by each paramter
            Raises:
                No reaises:
            Returns:
                : This function get pseudobinary message and make dataframe pandas
        """
        # FUNCION PARA DECODIFICAR TRAMA EN PSEUDOBINARIO. RECIVE COMO PARAMETROS:
        # 1.-CADENA A DECODIFICAR.
        # 2.- VECTOR CON LOS NUMEROS DE CARACTERES QUE CONFORMAN LOS DATOS.
        # 3.- VECTOR CON LOS MULTIPLICADORES DE LOS DATOS.

        VectorDecodificado = []
        contador = 0
        contadorMultiplicador = 0

        for NumeroCaracteres in caracteres:
            dato = cadena[contador:contador + NumeroCaracteres]
            datoCadena = ""
            # print(dato)
            if (dato != "///"):
                for datoActual in dato:
                    datoAc = format(ord(datoActual), 'b')
                    datoCadena = datoCadena + datoAc[-6:]
                # print(datoCadena)
                datoDecodificado = int(datoCadena, 2) / multiplicador[contadorMultiplicador]
                VectorDecodificado.append(datoDecodificado)
                # print(datoDecodificado)
                contadorMultiplicador = contadorMultiplicador + 1
                contador = contador + NumeroCaracteres
            else:
                contadorMultiplicador = contadorMultiplicador + 1
                contador = contador + NumeroCaracteres
                VectorDecodificado.append(" ")
        return VectorDecodificado

    def decompileChar(self, segmen):
        #print("original segmen test method : "+segmen)
        if (segmen != "///"):
            binMerged = "00"
            for i in bytearray(segmen, encoding='utf-8'):
                c2b = format(i, 'b')[1:]  # ascci to binary
                # c2b = c2b[1:] # remove firts character
                binMerged = binMerged + c2b
            ## take 4 charactaer from binary an conver to hexdecimal
            step = 4
            hexMerged = ""
            for i in range(0, 20, step):
                hexval = format(int(binMerged[i:i + step], 2), 'x')
                hexMerged += hexval
                #print(segmen,i, binMerged[i:i+step], hexval)
            decVal = int(hexMerged, 16)
            #print("codificado -> ;", segmen, "; | bin -> ;", binMerged, "; | hex -> ;", hexMerged, "; | dec ->;", decVal)
            return decVal
        else:
            return -1

    def decomMesage(self, text2decomp, decimals, nchars):
        """ This function decompile a psuedobinary message into clear text values.
            Args:
                test2decomp (string): String message to decompile.
                decimals (list): values to multiplicator each value to get number of decimal
                nchar (list): list with a number of characters by each value
            Raises:
                No Raises: No Raises implement yet.
            Returns:
               message (list) : The list with decompile message
        """
        print(text2decomp)
        message = []
        step = 3

        for var in nchars:
            print("Msg_Met_Decoder: decomMesage",var)


        for i in range(0, len(text2decomp), step):
            val=self.decompileChar(text2decomp[i:i + step])
            print(text2decomp[i:i + step], val)
            message.append(val)
        return message


if __name__ == '__main__':
    #@TODO Para poder leer los mensajes primero se debe hacer un split para separar los dos mensajes

    # test new decode
    # @DV@Cu@DC@AM@A@@AGBP{BPuBPx@@K@RO@F]@JL||I@Aa@@U@@|@Mn@ED@DZ@@L@@M@DR@DQ@DQ@@@@T~@Pc@~D@Ar}[{@@d@@Y@@Y@@Y@@W@SZ@CfBpY @C?@Cv@Cz@AM@AI@AKBPvBPqBPt@@@@Mp@Ep@GPXCY@AP@@M@@p@KE@EB@D\@@M@@o@DQ@DP@DQ@@@@UA@CT@L\@BGNqe@@K@@Y@@Y@@Y@@]@Nj@CgBsl
    # NESDIS : 3937659C  id_estation : 64447  estation : M5192
    cd = Msg_Met_Decoder()
    msg = "@DV@Cu@DC@AM@A@@AGBP{BPuBPx@@K@RO@F]@JL||I@Aa@@U@@|@Mn@ED@DZ@@L@@M@DR@DQ@DQ@@@@T~@Pc@~D@Ar}[{@@d@@Y@@Y@@Y@@W@SZ@CfBpY @C?@Cv@Cz@AM@AI@AKBPvBPqBPt@@@@Mp@Ep@GPXCY@AP@@M@@p@KE@EB@D\@@M@@o@DQ@DP@DQ@@@@UA@CT@L\@BGNqe@@K@@Y@@Y@@Y@@]@Nj@CgBsl"
    group_file =  pd.read_csv("../var_grupo5.csv", sep=";")
    #  msg, group_file, id_estacion,estacion,ftd
    ftd = datetime.strptime("10/25/2024 19:39:23", "%m/%d/%Y %H:%M:%S")
    insert = cd.bynary2text(msg,group_file,"3937659C",64447,"M5192",ftd)
    print(insert)

    # dc = Msg_Met_Decoder()
    # msj1 = "@Ak@BM@AX@AK@AS@@}AhLAhNAhK@@@@H[@QH@BiY|v@A}@BV@Au@An@Au@Aj@Ak@Al@Aj@An@An@An@Ak@Ak@Ak@Ar@Ar@Ar@Ap@Ap@Ap@Ch@BX@@v@@{@@K@@v@@A@A_@@I@@{@Vf @B@@BN@Au@AJ@AW@@?AhIAhLAhG@@@@P[@Qo@Jc[]o@Bn@B~@BV@B@@BJ@Au@Ao@Aq@Al@An@Ao@An@Ak@Ak@Aj@Ar@Ar@Ar@Ap@Ap@Ap@Lv@Ax@BU@@o@@w@A_@@I@Az@@J@@s@Uu"
    # msj2 = '@A\@Aa@AW@AX@A\@ATAgnAgoAgm@@@@C\@EF@BKAKR@Bc@Bf@B_@B\@B^@BZ@BJ@BJ@BI@A|@A}@A{@Am@Am@Am@Ar@Ar@Ar@An@An@An@Hm@Au@BO@AX@@f@AN@@Q@An@@P@@C@U}'
    # text2decomp = msj2
    #
    # inicio = time()
    # msg_txt = dc.decomMesage(text2decomp)
    # fin = time()
    # f1 = fin - inicio
    # print(len(msg_txt))
    #
    # for i in msg_txt:
    #     print(i)
    #
    # print("tiempo de ejecución funcion normal ", f1)
