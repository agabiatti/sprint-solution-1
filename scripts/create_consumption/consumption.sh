#!/bin/bash

echo "Start Consumption"

echo "Jogadores"
spark-submit create_consumption/jogadores.py

echo "Resultado Partidas"
spark-submit create_consumption/resultado_partidas.py

echo "Rodadas"
spark-submit create_consumption/rodadas.py