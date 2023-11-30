# Mcenter Streamli

## AWS
## Criando Instância
Crie uma Instância EC2 T2.micro com servidor ubuntu e utilize a regra de segurança launch-wizard-1.

## Configure o Servidor
Para instalar e configurar o projeto siga os passos a seguir em seu servidor. Utilize o terminal para adicionar os comandos abaixo:

```
sudo apt update
sudo apt install nginx -y
sudo apt install python3-pip -y
sudo apt upgrade -y
```

### Configure o git
[Siga o tutorial para configurar a chave SSH](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent)

Crie uma chave SSH
```
ssh-keygen -t ed25519 -C "coutinholps@gmail.com"
```

Adicione a chave ao agente ssh
```
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519
```

Copie a chave e aadicone as configurações do Github
```
cat ~/.ssh/id_ed25519.pub
```

### Clone o projeto
```
git clone git@github.com:lpcoutinho/mcenter_streamlit.git
```

### Defina as variáveid de ambiente
Caminhe até o diretório
```
cd mcenter_streamlit/
```

- Crie o arquivo **.env** na raiz do projeto e edite como no arquivo .env.example
- Crie o arquivo **secrets.toml** no diretório **.streamlit** e edite como no arquivo secrets.toml

### Permitindo sobreescrever arquivo
```
sudo chmod -R 777 .env
```

### Instale os pacotes necessários
```
sudo pip3 install -r requirements.txt
```


## Definindo crontab
### Dê permissões ao arquivo run.sh
```
chmod +x /home/ubuntu/mcenter_streamlit/app/pages/ml_consume/run.sh
chmod +x /home/ubuntu/mcenter_streamlit/app/pages/tiny_consume/run.sh
```

### Abra o arquivo para definir horários e rotinas
```
sudo crontab -e
```

### Edite o crontab
```
1 0 * * * /home/ubuntu/mcenter_streamlit/app/pages/ml_consume/run.sh >> /home/ubuntu/mcenter_streamlit/cron_ml.txt  2>&1
35 0 * * * /home/ubuntu/mcenter_streamlit/app/pages/tiny_consume/run.sh >> /home/ubuntu/mcenter_streamlit/cron_tiny.txt  2>&1
```

## Rode o projeto
Inicie um terminal tmux
```
tmux new -s start
```

Inicie o projeto
```
streamlit run app/1_🔑_Access_Token_ML.py 
```

Sempre que precisar voltar a esta sessão use
```
tmux a -t start
```
