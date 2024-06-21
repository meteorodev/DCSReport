# this script decompile seudobinary segment from aws-seudobinary-
import timeit
from time import time


class Msg_Met_Decoder():

    def decoPseudobinario(cadena, caracteres,
                          multiplicador):  # FUNCION PARA DECODIFICAR TRAMA EN PSEUDOBINARIO. RECIVE COMO PARAMETROS: 1.-CADENA A DECODIFICAR. 2.- VECTOR CON LOS NUMEROS DE CARACTERES QUE CONFORMAN LOS DATOS.  3.- VECTOR CON LOS MULTIPLICADORES DE LOS DATOS.
        VectorDecodificado = []
        contador = 0
        contadorMultiplicador = 0
        for NumeroCaracteres in caracteres:
            dato = cadena[contador:contador + NumeroCaracteres]
            datoCadena = ""
            print(dato)
            if (dato != "///"):
                for datoActual in dato:
                    datoAc = format(ord(datoActual), 'b')
                    datoCadena = datoCadena + datoAc[-6:]
                print(datoCadena)
                datoDecodificado = int(datoCadena, 2) / multiplicador[contadorMultiplicador]
                VectorDecodificado.append(datoDecodificado)
                print(datoDecodificado)
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


        # for i in range(0, len(text2decomp), step):
        #     val=self.decompileChar(text2decomp[i:i + step])
        #     print(text2decomp[i:i + step], val)
        #     message.append(val)
        return message


if __name__ == '__main__':
    #@TODO Para poder leer los mensajes primero se debe hacer un split para separar los dos mensajes

    dc = Msg_Met_Decoder()
    msj1 = "@Ak@BM@AX@AK@AS@@}AhLAhNAhK@@@@H[@QH@BiY|v@A}@BV@Au@An@Au@Aj@Ak@Al@Aj@An@An@An@Ak@Ak@Ak@Ar@Ar@Ar@Ap@Ap@Ap@Ch@BX@@v@@{@@K@@v@@A@A_@@I@@{@Vf @B@@BN@Au@AJ@AW@@?AhIAhLAhG@@@@P[@Qo@Jc[]o@Bn@B~@BV@B@@BJ@Au@Ao@Aq@Al@An@Ao@An@Ak@Ak@Aj@Ar@Ar@Ar@Ap@Ap@Ap@Lv@Ax@BU@@o@@w@A_@@I@Az@@J@@s@Uu"
    msj2 = '@A\@Aa@AW@AX@A\@ATAgnAgoAgm@@@@C\@EF@BKAKR@Bc@Bf@B_@B\@B^@BZ@BJ@BJ@BI@A|@A}@A{@Am@Am@Am@Ar@Ar@Ar@An@An@An@Hm@Au@BO@AX@@f@AN@@Q@An@@P@@C@U}'
    text2decomp = msj2

    inicio = time()
    msg_txt = dc.decomMesage(text2decomp)
    fin = time()
    f1 = fin - inicio
    print(len(msg_txt))

    for i in msg_txt:
        print(i)

    print("tiempo de ejecución funcion normal ", f1)
