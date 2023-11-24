#!/bin/sh
# Mude para o diretório do script
cd "$(dirname "$0")"

# Obtenha o diretório de trabalho atual
CWD="$(pwd)"
echo "Diretório de trabalho atual: $CWD"

# Execute o script Python
python3 routines.py
python3 test_script.py
