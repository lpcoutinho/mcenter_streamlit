# Mcenter Streamli

Para instalar e configurar o projeto siga os passos a seguir em seu servidor. Utilize o terminal para adicionar os comandos abaixo:
## Instalando projeto em servidor Ubuntu
```
sudo pip3 install -r requirements.txt
```

## Permitindo sobreescrever arquivo
```
sudo chmod -R 777 .env
```

## Definindo crontab
### Dê permissões ao arquivo run.sh
```
chmod +x /home/ubuntu/mcenter_streamlit/app/pages/ml_consume/run.sh
```

## Defina as variáveid de ambiente
- Crie o arquivo **.env** na raiz do projeto e edite como no arquivo .env.example
- Crie o arquivo **secrets.toml** no diretório **.streamlit** e edite como no arquivo secrets.toml

### Abra o arquivo para definir horários e rotinas
```
sudo crontab -e
```

### Edite o crontab
```
* * * * * /home/ubuntu/mcenter_streamlit/app/pages/ml_consume/run.sh >> /home/ubuntu/mcenter_streamlit/cron_ml.txt  2>&1
* * * * * /home/ubuntu/mcenter_streamlit/app/pages/tiny_consume/run.sh >> /home/ubuntu/mcenter_streamlit/cron_tiny.txt  2>&1
```

# Rode o projeto
