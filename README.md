Medivh
======

Get forecasts
-------------
* Default algorithm: `./medivh.py -c sample-config.yml -o result-default.csv`
* Mean algotuthm: `./medivh.py -c sample-config.yml -o result-mean.csv -a mean.csv`

Generate real sales data
------------------------
* `./tester.py -g -c sample-config.yml -o sales.csv`

Compare forecasts
-----------------
* `./tester.py -b -s sales.csv -f result-default.csv result-mean.csv -i plot.png`
