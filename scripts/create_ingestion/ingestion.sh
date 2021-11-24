#!/bin/bash

echo "Start Ingestion"

echo "Posições"
spark-submit create_ingestion/posicoes.py

echo "Times"
spark-submit create_ingestion/times.py

echo "Jogadores"
spark-submit create_ingestion/jogadores.py
spark-submit create_ingestion/jogadores_2018.py

echo "Pontuação Jogadores"
spark-submit create_ingestion/pontuacao_jogadores.py
spark-submit create_ingestion/pontuacao_jogadores_2014.py
spark-submit create_ingestion/pontuacao_jogadores_2017.py

echo "Resultado Partidas"
spark-submit create_ingestion/resultado_partidas.py

echo "Rodadas"
spark-submit create_ingestion/rodadas.py

