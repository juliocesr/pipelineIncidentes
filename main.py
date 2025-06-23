from scripts import processos

if __name__ == '__main__':
    processos.coletar_dados()
    processos.transformar_dados()
    processos.carregar_para_s3()

