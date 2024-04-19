# this script decompile seudobinary segment from aws-seudobinary-
import timeit
from time import time


class Msg_Met_Decoder():

    def decompileChar(self, segmen):
        # print("original segmen test method : "+segmen)
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
                # print(i, binMerged[i:i+step], hexval)
            decVal = int(hexMerged, 16)
            #print("codificado -> ;", segmen, "; | bin -> ;", binMerged, "; | hex -> ;", hexMerged, "; | dec ->;", decVal)
            return decVal
        else:
            return -1

    def decomMesage(self, text2decomp):
        message = []
        step = 3
        for i in range(0, len(text2decomp), step):
            self.decompileChar(text2decomp[i:i + step])
            message.append(self.decompileChar(text2decomp[i:i + step]))
        return message


if __name__ == '__main__':
    #@TODO Para poder leer los mensajes primero se debe hacer un split para separar los dos mensajes

    dc = Msg_Met_Decoder()
    msj1 = "@BA@BO@BF@AF@@?@ACAmBAl}Am@@@@@O[@Cm@EnBE@@CM@@r@AM///////////////////////////////////////////////////////////////@B?@Dh@A[@A_@@D@@m@U`"
    msj2 = '@BP@A?@BF@AE@@?@ABAl~AlyAl{@@@@QQ@CM@FeqnU@Cg@@l@AY///////////////////////////////////////////////////////////////@Cz@@a@Ab@A^@@E@@h@U\\'
    text2decomp = msj1

    inicio = time()
    msg_txt = dc.decomMesage(text2decomp)
    fin = time()
    f1 = fin - inicio
    print(len(msg_txt))

    for i in msg_txt:
        print(i)

    print("tiempo de ejecución funcion normal ", f1)
