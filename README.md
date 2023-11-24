# Mcenter Streamli

## Instalando projeto em servidor Ubuntu
```
sudo pip3 install streamlit
pip3 install -r requirements.txt
```

## Definindo crontab
### Dê permissões ao arquivo run.sh
```
chmod +x /home/ubuntu/mcenter_streamlit/app/pages/ml_consume/run.sh
```

### Abra o arquivo para definir horários e rotinas
```
sudo crontab -e
```

### Edite
```
* * * * * /home/ubuntu/mcenter_streamlit/app/pages/ml_consume/run.sh >> /home/ubuntu/mcenter_streamlit/cron_ml.txt  2>&1
* * * * * /home/ubuntu/mcenter_streamlit/app/pages/tiny_consume/run.sh >> /home/ubuntu/mcenter_streamlit/cron_tiny.txt  2>&1
```

# Rode o projeto
