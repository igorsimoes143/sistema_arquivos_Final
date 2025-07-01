# sistema_arquivos_Final

##Instruções para inicializar:

###Cliente: 
Inserir o ip onde o nameserver está localizado em os.environ["PYRO_BROADCAST_ADDRS"] = "172.16.5.82, 192.168.0.5, 192.168.3.92, 192.168.3.188"


###Demais componentes: 

Basta substituir o ip abaixo

ip_servidor = '192.168.0.5' -> substituir pelo ip onde rodará o servidor

ip_name_server = '192.168.0.5' -> substituir pelo ip onde rodará o nameserver


###Rodar o name server:
python -m Pyro5.nameserver -n hostname
Substituir hostname pelo ip onde rodará o nameserver

Os arquivos do cliente devem estar na pasta cliente, no mesmo nível que o script client.py

###Passos para devido funcionamento: (seguir nessa ordem)

python -m Pyro5.nameserver -n hostname
cd servidor/api_manager  ->  python api_manager.py
cd servidor/metadados  ->  python file_map.py
cd servidor/monitor  ->  python monitor.py
cd servidor/node1  -> python node1.py
cd servidor/node2  -> python node2.py
cd servidor/node3  -> python node3.py
cd servidor/node4  -> python node4.py
cd cliente  ->  python client.py
