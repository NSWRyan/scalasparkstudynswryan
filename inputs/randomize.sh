#!/bin/bash
venv

python run.py
python run2.py

rm -rf passengers.csv
rm -rf teleportData.csv
rm -rf id_map.csv

mv new_passenger.csv passengers.csv
mv teleportDataNew.csv teleportData.csv