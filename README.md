# sistema_arquivos_Final

Instruções para inicializar:

Cliente: Inserir o ip onde o nameserver está localizado em os.environ["PYRO_BROADCAST_ADDRS"] = "172.16.5.82, 192.168.0.5, 192.168.3.92, 192.168.3.188"


Demais componentes: Basta substituir o ip em

# endereço IP do servidor, substituir pelo ip da máquina que rodará o servidor
ip_servidor = '192.168.0.5'

#Endereço ip do name_server 
ip_name_server = '192.168.0.5'

pelo ip em que o servidor rodará e onde o nameserver está localizado respectivamente


Para rodar o nameserver: python -m Pyro5.nameserver -n hostname
Substituir hostname pelo ip onde rodará o nameserver

Para enviar algum arquivo, o arquivo deve estar na pasta cliente

Para funcionamento devido, o arquivos .py devem rodar nessa ordem:
ex: 
cd servidor/api_manager  ->  python api_manager.py
cd servidor/metadados  ->  python file_map.py
cd servidor/monitor  ->  python monitor.py
cd servidor/node1  -> python node1.py
cd servidor/node2  -> python node2.py
cd servidor/node3  -> python node3.py
cd servidor/node4  -> python node4.py
cd cliente  ->  python client.py