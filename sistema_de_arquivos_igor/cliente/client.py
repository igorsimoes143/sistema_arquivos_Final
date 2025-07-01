import Pyro5.client
import Pyro5.api
import os
import base64
from pathlib import Path
import time

CHUNK_SIZE = 1024

def main():

    Pyro5.config.SERIALIZER = "serpent"
    #procura por broadcast os ips separados por vírgula registrados nesse campo
    #Adicionar IP do name_server aqui
    os.environ["PYRO_BROADCAST_ADDRS"] = "172.16.5.82, 192.168.0.5, 192.168.3.92, 192.168.3.188"

    #Localiza o sevidor de nomes por broadcast através da criação de um proxy para o servidor de nomes.
    #Como o ip está registrado em "PYRO_BROADCAST_ADDRS", ele buscará por broadcast (sem argumentos em
    #locate_ns()) o servidor de nomes.
    nameserver = Pyro5.api.locate_ns()

    #Procura pelo nome lógico do objeto remoto, retorna um uri, que
    #contêm o local (ip e porta do objeto remoto e seu id)

    try:
        uri2 = nameserver.lookup("api_manager")
    except Pyro5.errors.NamingError as e:
        print(f"Erro: Não foi possível encontrar um dos servidores de arquivos no Name Server. {e}")
        return

    #Cria um proxy no cliente para servir de "objeto local" da calculadora,
    #mas que na verdade faz a comunicação com o daemon do objeto remoto no servidor
    #para efetuar a invocação remota de métodos

    sistema_api=Pyro5.client.Proxy(uri2)

    while True:
        print("\n=== Sistema de arquivos DISTRIBUÍDO ===")
        print("1. listar")
        print("2. inserir")
        print("3. leitura")
        print("4. remover")
        print("0. Sair")

        opcao = input("Escolha uma operação: ")

        if opcao == "0":
            print("Encerrando...")
            break

        try:
            if opcao == "1":
                try:
                    print("\n-----------------Arquivos-----------------")
                    resultado = sistema_api.listar()
                    print(resultado)
                except Exception as e:
                    print(f"\nOcorreu um erro ao chamar o servidor:")
                    print(f"  - Tipo do Erro: {type(e)}")
                    print(f"  - Mensagem: {e}")
            elif opcao == "2":
                try:
                    nome_arq = input("Digite o nome do arquivo para enviar: ").strip()
                    caminho_local = Path(nome_arq)

                    if not caminho_local.is_file():
                        raise FileNotFoundError(f"Arquivo '{nome_arq}' não encontrado localmente.")
                    
                    # Lógica de enviar em chunks para o API Manager
                    with open(caminho_local, "rb") as f:
                        indice = 1
                        while True:
                            chunk = f.read(CHUNK_SIZE)
                            if not chunk:
                                break
                            
                            sistema_api.receber_chunk(nome_arq, chunk)
                            indice += 1

                    # Finaliza o processo
                    print("Todos os chunks enviados. Finalizando o upload...")
                    mensagem_final, tempo = sistema_api.finalizar_upload(nome_arq)
                    tamanho_bytes = os.path.getsize(caminho_local)
                    print(f"Resposta do Servidor: Envio concluido com sucesso em {tempo} segundos ({tamanho_bytes/1024/1024/tempo} mb/s)")
        
                except Exception as e:
                    print(f"Erro durante o upload: {e}")
            elif opcao == "3":
                try:            
                    nome_arq = input("Digite o nome do arquivo que deseja baixar: ").strip()
                    inicio_operacao = time.time() # Marca o tempo ANTES da operação
                    dados_do_arquivo = sistema_api.ler(nome_arq)
                    fim_operacao = time.time()   # Marca o tempo DEPOIS da operação
                    tempo_decorrido = fim_operacao - inicio_operacao

                    dados_do_arquivo = dados_do_arquivo["data"]
                    bytes_do_arquivo = base64.b64decode(dados_do_arquivo)
                    if bytes_do_arquivo:
                        # 2. Salva os bytes recebidos em um arquivo local
                        nome_local = f"download_{nome_arq}"
                        print(f"Recebidos {len(bytes_do_arquivo)/1024/1024} MB. Salvando como '{nome_local}'...")
                        caminho_local_obj = Path(nome_local)
                        caminho_local_obj.write_bytes(bytes_do_arquivo)
                        tamanho_bytes = caminho_local_obj.stat().st_size
                        print("Download concluído com sucesso!")
                        print(f"Resposta do Servidor: {tempo_decorrido} segundos ({tamanho_bytes/1024/1024/tempo_decorrido} mb/s)")
                    else:
                        print("Servidor retornou uma resposta vazia, o que é inesperado.")

                except Exception as e:
                    # Captura qualquer erro que o servidor tenha enviado (FileNotFoundError, RuntimeError, etc.)
                    print(f"Ocorreu um erro durante o download: {e}")

            elif opcao == "4":
                try:             
                    nome_arq = input("Digite o nome do arquivo que deseja remover: ").strip()
                    inicio_operacao = time.time() # Marca o tempo ANTES da operação
                    resultado = sistema_api.remover(nome_arq)
                    fim_operacao = time.time()   # Marca o tempo DEPOIS da operação
                    tempo_decorrido = fim_operacao - inicio_operacao
                    print(f"Resposta do Servidor: Remoção concluída com sucesso em {tempo_decorrido} segundos")
                except Exception as e:
                    # Captura qualquer erro que o servidor tenha enviado (FileNotFoundError, RuntimeError, etc.)
                    print(f"Ocorreu um erro durante a remoção: {e}")
            else:
                print("Opção inválida!")
                continue

        except Exception as e:
            print(f"Erro durante a operação: {e}")


main()
